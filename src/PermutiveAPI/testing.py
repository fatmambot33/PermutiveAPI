"""Deterministic local HTTP fixtures for PermutiveAPI integrations."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Deque, Dict, Mapping, Optional, Tuple, cast
from urllib.parse import parse_qs, urlsplit

from .sdk import JSONValue

MOCK_FIXTURE_VERSION = 1


@dataclass(frozen=True)
class MockResponse:
    """Define one queued local HTTP response."""

    status_code: int = 200
    body: Optional[JSONValue] = None
    headers: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible fixture representation."""
        return {
            "status_code": self.status_code,
            "body": self.body,
            "headers": dict(self.headers),
        }


@dataclass(frozen=True)
class MockRoute:
    """Map one method and path to one or more queued responses."""

    method: str
    path: str
    responses: Tuple[MockResponse, ...]

    def __post_init__(self) -> None:
        """Validate the route contract."""
        if not self.path.startswith("/"):
            raise ValueError("Mock route paths must start with '/'.")
        if not self.responses:
            raise ValueError("Mock routes require at least one response.")

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible fixture representation."""
        return {
            "method": self.method.upper(),
            "path": self.path,
            "responses": [response.to_dict() for response in self.responses],
        }


@dataclass(frozen=True)
class MockRequest:
    """Record one request received by the local server."""

    method: str
    path: str
    query: Mapping[str, Tuple[str, ...]]
    headers: Mapping[str, str]
    body: Optional[JSONValue]


class _FixtureHTTPServer(ThreadingHTTPServer):
    """Threaded HTTP server with typed route queues and request capture."""

    daemon_threads = True

    def __init__(self, routes: Tuple[MockRoute, ...]) -> None:
        super().__init__(("127.0.0.1", 0), _FixtureRequestHandler)
        self._lock = Lock()
        self._routes: Dict[Tuple[str, str], Deque[MockResponse]] = {
            (route.method.upper(), route.path): deque(route.responses)
            for route in routes
        }
        self._requests: list[MockRequest] = []

    def response_for(self, method: str, path: str) -> MockResponse:
        """Return the next response, retaining the final response for reuse."""
        key = (method.upper(), path)
        with self._lock:
            responses = self._routes.get(key)
            if responses is None:
                return MockResponse(
                    status_code=404,
                    body={"error": "mock_route_not_found", "path": path},
                )
            if len(responses) > 1:
                return responses.popleft()
            return responses[0]

    def record(self, request: MockRequest) -> None:
        """Append one immutable request record."""
        with self._lock:
            self._requests.append(request)

    def request_snapshot(self) -> Tuple[MockRequest, ...]:
        """Return an immutable request-log snapshot."""
        with self._lock:
            return tuple(self._requests)


class _FixtureRequestHandler(BaseHTTPRequestHandler):
    """Dispatch HTTP methods to the fixture server."""

    server_version = "PermutiveAPIMock/1"

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        self._handle()

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        self._handle()

    def do_PUT(self) -> None:  # noqa: N802 - stdlib handler contract
        self._handle()

    def do_PATCH(self) -> None:  # noqa: N802 - stdlib handler contract
        self._handle()

    def do_DELETE(self) -> None:  # noqa: N802 - stdlib handler contract
        self._handle()

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler contract
        self._handle(include_body=False)

    def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib handler contract
        self._handle()

    def log_message(self, _format: str, *_args: object) -> None:
        """Disable noisy request logging in deterministic tests."""

    def _handle(self, *, include_body: bool = True) -> None:
        server = cast(_FixtureHTTPServer, self.server)
        parsed = urlsplit(self.path)
        body = self._read_body()
        request = MockRequest(
            method=self.command,
            path=parsed.path,
            query={
                key: tuple(values)
                for key, values in parse_qs(
                    parsed.query,
                    keep_blank_values=True,
                ).items()
            },
            headers={key: value for key, value in self.headers.items()},
            body=body,
        )
        server.record(request)
        response = server.response_for(self.command, parsed.path)
        encoded = (
            json.dumps(response.body, sort_keys=True).encode("utf-8")
            if response.body is not None
            else b""
        )
        self.send_response(response.status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        for key, value in response.headers.items():
            self.send_header(key, value)
        self.end_headers()
        if include_body and encoded:
            self.wfile.write(encoded)

    def _read_body(self) -> Optional[JSONValue]:
        length_value = self.headers.get("Content-Length")
        if length_value is None:
            return None
        try:
            length = int(length_value)
        except ValueError:
            return None
        if length <= 0:
            return None
        raw = self.rfile.read(length).decode("utf-8")
        try:
            return cast(JSONValue, json.loads(raw))
        except json.JSONDecodeError:
            return raw


class MockPermutiveServer:
    """Run deterministic Permutive-like fixtures on a loopback HTTP server."""

    def __init__(self, routes: Tuple[MockRoute, ...]) -> None:
        self._server = _FixtureHTTPServer(routes)
        self._thread: Optional[Thread] = None

    @classmethod
    def standard(cls) -> "MockPermutiveServer":
        """Create a server with the canonical versioned fixture catalog."""
        return cls(standard_mock_routes())

    @property
    def base_url(self) -> str:
        """Return the active loopback base URL."""
        address = cast(Tuple[str, int], self._server.server_address)
        return f"http://{address[0]}:{address[1]}"

    @property
    def requests(self) -> Tuple[MockRequest, ...]:
        """Return an immutable request-log snapshot."""
        return self._server.request_snapshot()

    def url(self, path: str) -> str:
        """Build one absolute local fixture URL."""
        return f"{self.base_url}/{path.lstrip('/')}"

    def start(self) -> "MockPermutiveServer":
        """Start the loopback server exactly once."""
        if self._thread is not None:
            return self
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def close(self) -> None:
        """Stop the server and release its loopback socket."""
        if self._thread is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)
        self._thread = None

    def __enter__(self) -> "MockPermutiveServer":
        """Start and return the local server context."""
        return self.start()

    def __exit__(self, *_: object) -> None:
        """Close the local server context."""
        self.close()


def standard_mock_routes() -> Tuple[MockRoute, ...]:
    """Return the canonical version 1 Permutive fixture routes."""
    return (
        MockRoute(
            "GET",
            "/v1/success",
            (MockResponse(body={"id": "fixture-success", "state": "ready"}),),
        ),
        MockRoute(
            "POST",
            "/v1/create",
            (MockResponse(status_code=201, body={"id": "fixture-created"}),),
        ),
        MockRoute(
            "POST",
            "/v1/validation",
            (MockResponse(status_code=400, body={"error": "invalid_fixture"}),),
        ),
        MockRoute(
            "GET",
            "/v1/authentication",
            (MockResponse(status_code=401, body={"error": "authentication_failed"}),),
        ),
        MockRoute(
            "GET",
            "/v1/not-found",
            (MockResponse(status_code=404, body={"error": "not_found"}),),
        ),
        MockRoute(
            "POST",
            "/v1/conflict",
            (MockResponse(status_code=409, body={"error": "conflict"}),),
        ),
        MockRoute(
            "GET",
            "/v1/rate-limit",
            (
                MockResponse(
                    status_code=429,
                    body={"error": "rate_limited"},
                    headers={"Retry-After": "0"},
                ),
                MockResponse(body={"state": "recovered", "attempt": 2}),
            ),
        ),
        MockRoute(
            "GET",
            "/v1/server-retry",
            (
                MockResponse(status_code=500, body={"error": "temporary_failure"}),
                MockResponse(body={"state": "recovered", "attempt": 2}),
            ),
        ),
        MockRoute(
            "GET",
            "/v1/server-failure",
            (MockResponse(status_code=500, body={"error": "server_failure"}),),
        ),
        MockRoute(
            "GET",
            "/v1/pagination",
            (
                MockResponse(
                    body={
                        "items": [{"id": "page-1"}],
                        "continuation": "fixture-page-2",
                    }
                ),
                MockResponse(body={"items": [{"id": "page-2"}]}),
            ),
        ),
        MockRoute(
            "GET",
            "/v1/repeated-token",
            (
                MockResponse(
                    body={
                        "items": [{"id": "repeat-1"}],
                        "continuation": "fixture-repeat",
                    }
                ),
                MockResponse(
                    body={
                        "items": [{"id": "repeat-2"}],
                        "continuation": "fixture-repeat",
                    }
                ),
            ),
        ),
    )


def mock_fixture_catalog() -> dict[str, object]:
    """Return the versioned machine-readable fixture catalog."""
    return {
        "version": MOCK_FIXTURE_VERSION,
        "routes": [route.to_dict() for route in standard_mock_routes()],
    }


__all__ = [
    "MOCK_FIXTURE_VERSION",
    "MockPermutiveServer",
    "MockRequest",
    "MockResponse",
    "MockRoute",
    "mock_fixture_catalog",
    "standard_mock_routes",
]
