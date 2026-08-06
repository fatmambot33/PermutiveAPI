"""Sanitized HTTP recording and deterministic transport replay."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from requests import Response

from .sdk import JSONValue, Transport

RECORDING_FORMAT_VERSION = 1
_SAFE_RESPONSE_HEADERS = frozenset(
    {"content-type", "retry-after", "x-request-id", "request-id"}
)
_SENSITIVE_KEYS = (
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "password",
    "secret",
    "token",
)


@dataclass(frozen=True)
class RecordedInteraction:
    """Describe one secret-safe HTTP response interaction."""

    method: str
    endpoint: str
    status_code: int
    headers: Mapping[str, str] = field(default_factory=dict)
    body: JSONValue = None

    def __post_init__(self) -> None:
        """Normalize and validate one replay interaction."""
        method = self.method.upper()
        if not method:
            raise ValueError("Recorded interaction method must not be empty.")
        if "?" in self.endpoint or "#" in self.endpoint:
            raise ValueError("Recorded endpoints must not contain query or fragment data.")
        if self.status_code < 100 or self.status_code > 599:
            raise ValueError("Recorded status codes must be valid HTTP statuses.")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "headers", dict(sorted(self.headers.items())))

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable recording data."""
        return {
            "method": self.method,
            "endpoint": self.endpoint,
            "status_code": self.status_code,
            "headers": dict(self.headers),
            "body": self.body,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "RecordedInteraction":
        """Create one validated interaction from decoded JSON."""
        method = value.get("method")
        endpoint = value.get("endpoint")
        status_code = value.get("status_code")
        headers = value.get("headers", {})
        if not isinstance(method, str):
            raise TypeError("Recorded method must be a string.")
        if not isinstance(endpoint, str):
            raise TypeError("Recorded endpoint must be a string.")
        if not isinstance(status_code, int) or isinstance(status_code, bool):
            raise TypeError("Recorded status_code must be an integer.")
        if not isinstance(headers, dict) or not all(
            isinstance(key, str) and isinstance(item, str)
            for key, item in headers.items()
        ):
            raise TypeError("Recorded headers must be a string mapping.")
        body = _json_value(value.get("body"))
        return cls(method, endpoint, status_code, headers, body)


@dataclass(frozen=True)
class Recording:
    """Contain one versioned deterministic HTTP recording."""

    interactions: tuple[RecordedInteraction, ...]
    version: int = RECORDING_FORMAT_VERSION

    def __post_init__(self) -> None:
        """Validate the recording format version."""
        if self.version != RECORDING_FORMAT_VERSION:
            raise ValueError(f"Unsupported recording version: {self.version}")

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable recording data."""
        return {
            "version": self.version,
            "interactions": [item.to_dict() for item in self.interactions],
        }

    def to_json(self) -> str:
        """Serialize the recording deterministically."""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"

    def write(self, path: Path) -> None:
        """Write a deterministic UTF-8 recording file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "Recording":
        """Create a validated recording from decoded JSON."""
        version = value.get("version")
        interactions = value.get("interactions")
        if not isinstance(version, int) or isinstance(version, bool):
            raise TypeError("Recording version must be an integer.")
        if not isinstance(interactions, list):
            raise TypeError("Recording interactions must be a list.")
        return cls(
            interactions=tuple(
                RecordedInteraction.from_dict(_mapping(item))
                for item in interactions
            ),
            version=version,
        )

    @classmethod
    def read(cls, path: Path) -> "Recording":
        """Read and validate one recording file."""
        decoded: object = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(_mapping(decoded))


class RecordingTransport:
    """Wrap a transport and capture secret-safe deterministic responses."""

    def __init__(self, transport: Transport) -> None:
        self.transport = transport
        self._interactions: list[RecordedInteraction] = []

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Send a request and record only sanitized response evidence."""
        response = self.transport.request(method, url, **kwargs)
        self._interactions.append(
            RecordedInteraction(
                method=method,
                endpoint=_safe_endpoint(url),
                status_code=response.status_code,
                headers=_safe_headers(response.headers),
                body=_response_body(response),
            )
        )
        return response

    @property
    def recording(self) -> Recording:
        """Return an immutable snapshot of captured interactions."""
        return Recording(tuple(self._interactions))


class ReplayMismatchError(AssertionError):
    """Report deterministic request/replay mismatches."""


class ReplayTransport:
    """Replay a finite sequence of recorded responses in strict order."""

    def __init__(self, recording: Recording) -> None:
        self.recording = recording
        self._remaining: Deque[RecordedInteraction] = deque(recording.interactions)

    @property
    def remaining(self) -> int:
        """Return the number of responses that have not been replayed."""
        return len(self._remaining)

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Return the next response when method and safe endpoint match."""
        del kwargs
        if not self._remaining:
            raise ReplayMismatchError("Recording is exhausted.")
        interaction = self._remaining.popleft()
        actual_method = method.upper()
        actual_endpoint = _safe_endpoint(url)
        if (
            actual_method != interaction.method
            or actual_endpoint != interaction.endpoint
        ):
            raise ReplayMismatchError(
                "Replay request mismatch: "
                f"expected {interaction.method} {interaction.endpoint}, "
                f"got {actual_method} {actual_endpoint}."
            )
        response = Response()
        response.status_code = interaction.status_code
        response.url = url
        response.headers.update(interaction.headers)
        response._content = (
            b""
            if interaction.body is None
            else json.dumps(interaction.body, sort_keys=True).encode("utf-8")
        )
        return response


def sanitize_json(value: JSONValue) -> JSONValue:
    """Redact sensitive keys recursively while preserving response structure."""
    if isinstance(value, list):
        return [sanitize_json(item) for item in value]
    if isinstance(value, dict):
        return {
            key: (
                "[REDACTED]"
                if _sensitive_key(key)
                else sanitize_json(item)
            )
            for key, item in value.items()
        }
    return value


def _safe_endpoint(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _safe_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {
        key: value
        for key, value in sorted(headers.items())
        if key.lower() in _SAFE_RESPONSE_HEADERS
    }


def _response_body(response: Response) -> JSONValue:
    if not response.content:
        return None
    try:
        decoded: object = response.json()
    except ValueError:
        return None
    return sanitize_json(_json_value(decoded))


def _sensitive_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_KEYS)


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in value
    ):
        raise TypeError("Expected a JSON object.")
    return value


def _json_value(value: object) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("JSON object keys must be strings.")
        return {str(key): _json_value(item) for key, item in value.items()}
    raise TypeError(f"Unsupported JSON value type: {type(value).__name__}")


__all__ = [
    "RECORDING_FORMAT_VERSION",
    "RecordedInteraction",
    "Recording",
    "RecordingTransport",
    "ReplayMismatchError",
    "ReplayTransport",
    "sanitize_json",
]
