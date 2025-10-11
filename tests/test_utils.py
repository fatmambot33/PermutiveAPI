from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from unittest.mock import Mock
import uuid
import urllib.parse
import time
from typing import Any, Callable, Dict, List, Tuple

import pytest
from PermutiveAPI._Utils.http import (
    BatchRequest,
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
    process_batch,
)
from requests.exceptions import RequestException
from requests.models import PreparedRequest, Response
from requests.structures import CaseInsensitiveDict

from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.json import (
    JSONSerializable,
    json_default,
)
from PermutiveAPI._Utils.file import check_filepath, split_filepath
from PermutiveAPI._Utils.list import (
    chunk_list,
    convert_list,
    compare_list,
    merge_list,
)


class Color(Enum):
    RED = "red"


@dataclass
class Dummy(JSONSerializable[Dict[str, Any]]):
    id: int
    name: str
    values: list[int] | None = None


class MockResponse(Response):
    def __bool__(self) -> bool:  # pragma: no cover
        return True


def _make_response(
    url: str, status: int, content: bytes = b"", headers: dict | None = None
) -> Response:
    """Create a mock `requests.Response` object for testing."""
    req = PreparedRequest()
    req.prepare_url(url, None)
    resp = MockResponse()
    resp.status_code = status
    resp._content = content
    resp.request = req
    resp.headers = CaseInsensitiveDict(headers or {})
    return resp


def test_redact_sensitive_data():
    """Test redaction of sensitive data from URLs and messages."""
    # Test redacting from URL query parameters
    url_with_key = "https://api.com/resource?api_key=secret_key&other=val"
    resp = _make_response(url_with_key, 400)

    # Test message containing sensitive key-value pairs
    message_with_secrets = 'some info api_key=secret_key and token="secret_token"'

    redacted_message = http._redact_sensitive_data(message_with_secrets, resp)
    assert "secret_key" not in redacted_message
    assert "secret_token" not in redacted_message
    assert "[REDACTED]" in redacted_message

    # Test with no sensitive data
    url_without_key = "https://api.com/resource?other=val"
    resp_no_secret = _make_response(url_without_key, 200)
    message_without_secrets = "this is fine"
    redacted_message = http._redact_sensitive_data(
        message_without_secrets, resp_no_secret
    )
    assert message_without_secrets == redacted_message
    assert "[REDACTED]" not in redacted_message


def test_extract_error_message():
    """Test extraction of error messages from response body."""
    # Test valid JSON error
    resp_valid = _make_response(
        "https://api.com", 400, b'{"error":{"cause":"bad request"}}'
    )
    assert http._extract_error_message(resp_valid) == "bad request"

    # Test malformed JSON error
    resp_malformed = _make_response("https://api.com", 400, b'{"error": "cause"}')
    assert http._extract_error_message(resp_malformed) == "Unknown error"

    # Test non-JSON error
    resp_not_json = _make_response("https://api.com", 400, b"not json")
    assert http._extract_error_message(resp_not_json) == "Could not parse error message"


@pytest.mark.parametrize(
    "status, expected_exception",
    [
        (400, PermutiveBadRequestError),
        (401, PermutiveAuthenticationError),
        (403, PermutiveAuthenticationError),
        (404, PermutiveResourceNotFoundError),
        (429, PermutiveRateLimitError),
        (500, PermutiveServerError),
        (503, PermutiveServerError),
    ],
)
def test_handle_exception_raises(status, expected_exception):
    """Test that handle_exception raises the correct custom exception."""
    url = "https://api.com/test?api_key=secret"
    resp = _make_response(url, status, b'{"error":{"cause":"bad request"}}')

    with pytest.raises(expected_exception) as excinfo:
        http.raise_for_status(Exception("boom"), resp)

    # For 400, check that the message contains the URL and that it's redacted
    if status == 400:
        assert "bad request" in str(excinfo.value)
        assert "secret" not in str(excinfo.value)
        assert "[REDACTED]" in str(excinfo.value)


def test_handle_exception_no_response():
    """Test that a generic error is raised when there is no response."""
    with pytest.raises(PermutiveAPIError, match="An unexpected error occurred"):
        http.raise_for_status(Exception("boom"), None)


def test_list_helpers():
    """Test various list utility functions."""
    assert chunk_list([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert convert_list("[1, 2]") == [1, 2]
    assert convert_list([1, 2]) == [1, 2]
    assert compare_list(["a", "b"], ["b", "a"]) is True
    assert merge_list([1, 2], [2, 3]) == [1, 2, 3]
    assert merge_list([1], 2) == [1, 2]
    assert merge_list([1], None) == [1]


def test_file_helpers(tmp_path):
    """Test filepath utility functions."""
    # Test check_filepath
    dir_path = tmp_path / "test_dir"
    file_path = dir_path / "test.txt"
    check_filepath(file_path)
    assert dir_path.exists()

    # Test split_filepath
    path, name, ext = split_filepath(file_path)
    assert path == str(dir_path)
    assert name == "test"
    assert ext == ".txt"

    path_str, *_ = split_filepath(str(file_path))
    assert path_str == str(dir_path)


def test_json_default():
    """Test the custom JSON serializer default function."""
    u = uuid.uuid4()
    now = datetime(2021, 1, 2, 3, 4, 5)
    d = date(2021, 1, 2)

    class Obj:
        def __init__(self):
            self.x = 1

    assert json_default(Color.RED) == "red"
    assert json_default(Decimal("1.2")) == pytest.approx(1.2)
    assert json_default(5) == 5
    assert json_default(u) == str(u)
    assert json_default(now) == now.isoformat()
    assert json_default(d) == {"year": 2021, "month": 1, "day": 2}
    assert json_default([1, 2]) == [1, 2]
    assert json_default({"a": 1}) == {"a": 1}
    assert json_default(Obj()) == {"x": 1}
    assert json_default(None) is None
    assert json_default(b"b") == "b'b'"


def test_json_serializable(tmp_path):
    """Test the JSONSerializable base class for an object."""
    dummy = Dummy(1, "a", [1, 2])
    path = tmp_path / "dummy.json"
    dummy.to_json_file(path)
    assert Dummy.from_json_file(path) == dummy
    assert str(dummy).startswith("{")

    json_str = json.dumps(dummy.to_json())
    assert Dummy.from_json(json_str) == dummy
    assert Dummy.from_json(path) == dummy
    assert Dummy.from_json({"id": 1, "name": "a", "values": [1, 2]}) == dummy

    with pytest.raises(TypeError):
        Dummy.from_json(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Dummy.from_json("not json")


def test_process_batch_multiple_successes(fake_thread_pool, monkeypatch):
    """Ensure ``process_batch`` handles multiple successes and tracks progress."""

    recorded_calls = []

    def fake_request(method, api_key, url, **kwargs):  # noqa: ANN001 - mirror signature
        recorded_calls.append((method, api_key, url, kwargs))
        response = Mock(spec=Response)
        response.url = url
        response.payload = kwargs.get("json", {})
        return response

    monkeypatch.setattr(http, "request", fake_request)

    progress_updates: List[http.Progress] = []
    batch_requests = [
        BatchRequest(method="POST", url="https://example.com/one", json={"idx": 1}),
        BatchRequest(method="POST", url="https://example.com/two", json={"idx": 2}),
        BatchRequest(method="POST", url="https://example.com/three", json={"idx": 3}),
    ]

    responses, errors = process_batch(
        batch_requests,
        api_key="test-key",
        max_workers=2,
        progress_callback=progress_updates.append,
    )

    assert len(fake_thread_pool) == 1
    assert fake_thread_pool[0].max_workers == 2
    assert len(recorded_calls) == len(batch_requests)
    assert len(responses) == len(batch_requests)
    assert errors == []

    assert len(progress_updates) == len(batch_requests)
    assert [p.completed for p in progress_updates] == [1, 2, 3]
    assert all(p.total == len(batch_requests) for p in progress_updates)
    assert all(p.errors == 0 for p in progress_updates)
    assert {id(p.batch_request) for p in progress_updates} == {
        id(b) for b in batch_requests
    }
    assert all(
        p.average_per_thousand_seconds is None or p.average_per_thousand_seconds >= 0
        for p in progress_updates
    )


def test_process_batch_aggregates_errors(fake_thread_pool, monkeypatch):
    """Verify errors are aggregated and callbacks execute correctly."""

    success_callbacks: List[str] = []
    error_callbacks: List[Tuple[str, Exception]] = []

    def fake_request(method, api_key, url, **kwargs):  # noqa: ANN001 - mirror signature
        if url.endswith("/fail"):
            raise PermutiveAPIError("boom")
        response = Mock(spec=Response)
        response.url = url
        return response

    monkeypatch.setattr(http, "request", fake_request)

    def make_success(label: str) -> Callable[[Response], None]:
        def _callback(response: Response) -> None:
            success_callbacks.append(label)

        return _callback

    def make_error(label: str) -> Callable[[Exception], None]:
        def _callback(exc: Exception) -> None:
            error_callbacks.append((label, exc))

        return _callback

    failing_request = BatchRequest(
        method="GET",
        url="https://example.com/fail",
        error_callback=make_error("fail"),
    )
    batch_requests = [
        BatchRequest(
            method="GET",
            url="https://example.com/success-1",
            callback=make_success("one"),
        ),
        failing_request,
        BatchRequest(
            method="GET",
            url="https://example.com/success-2",
            callback=make_success("two"),
        ),
    ]

    progress_updates: List[http.Progress] = []
    responses, aggregated_errors = process_batch(
        batch_requests,
        api_key="test-key",
        max_workers=1,
        progress_callback=progress_updates.append,
    )

    assert len(fake_thread_pool) == 1
    assert fake_thread_pool[0].max_workers == 1
    assert len(responses) == 2
    assert sorted(success_callbacks) == ["one", "two"]
    assert len(success_callbacks) == 2

    assert len(aggregated_errors) == 1
    recorded_request, recorded_exc = aggregated_errors[0]
    assert recorded_request is failing_request
    assert isinstance(recorded_exc, PermutiveAPIError)
    assert error_callbacks == [("fail", recorded_exc)]

    assert len(progress_updates) == len(batch_requests)
    assert [p.completed for p in progress_updates] == [1, 2, 3]
    error_counts = [p.errors for p in progress_updates]
    assert error_counts[-1] == len(aggregated_errors)
    assert all(0 <= count <= len(aggregated_errors) for count in error_counts)


def test_process_batch_respects_retry(fake_thread_pool, monkeypatch):
    """Simulate a retried request to ensure eventual success is captured."""

    attempt_counter = {"count": 0}

    def fake_post(url, **kwargs):  # noqa: ANN001 - signature mirrors requests.post
        attempt_counter["count"] += 1
        if attempt_counter["count"] == 1:
            return _make_response(url, 500, headers={"Retry-After": "0"})
        return _make_response(url, 200)

    monkeypatch.setattr(http.requests, "post", fake_post)
    monkeypatch.setattr(http.time, "sleep", lambda *args, **kwargs: None)

    progress_updates: List[http.Progress] = []
    responses, errors = process_batch(
        [BatchRequest(method="POST", url="https://example.com/retry")],
        api_key="test-key",
        max_workers=1,
        progress_callback=progress_updates.append,
    )

    assert attempt_counter["count"] == 2
    assert len(fake_thread_pool) == 1
    assert fake_thread_pool[0].max_workers == 1
    assert len(responses) == 1
    assert errors == []
    assert len(progress_updates) == 1
    assert progress_updates[0].errors == 0


def test_process_batch_uses_env_max_workers(fake_thread_pool, monkeypatch):
    """Use the environment when ``max_workers`` is omitted."""

    def fake_request(method, api_key, url, **kwargs):  # noqa: ANN001 - mirror helper
        response = Mock(spec=Response)
        response.url = url
        return response

    monkeypatch.setenv("PERMUTIVE_BATCH_MAX_WORKERS", "6")
    monkeypatch.setattr(http, "request", fake_request)

    responses, errors = process_batch(
        [BatchRequest(method="GET", url="https://example.com/env")],
        api_key="test-key",
        max_workers=None,
        progress_callback=None,
    )

    assert len(fake_thread_pool) == 1
    assert fake_thread_pool[0].max_workers == 6
    assert len(responses) == 1
    assert errors == []


def test_process_batch_rejects_invalid_env_max_workers(monkeypatch):
    """Surface invalid ``PERMUTIVE_BATCH_MAX_WORKERS`` configuration."""

    monkeypatch.setenv("PERMUTIVE_BATCH_MAX_WORKERS", "not-a-number")

    with pytest.raises(ValueError, match="PERMUTIVE_BATCH_MAX_WORKERS"):
        process_batch(
            [BatchRequest(method="GET", url="https://example.com/env")],
            api_key="test-key",
            max_workers=None,
            progress_callback=None,
        )


def test_batch_request_timeout_env_default(monkeypatch):
    """Default batch timeout follows the environment override when provided."""

    monkeypatch.delenv("PERMUTIVE_BATCH_TIMEOUT_SECONDS", raising=False)
    request_default = BatchRequest(method="GET", url="https://example.com/timeout")
    assert request_default.timeout == 10.0

    monkeypatch.setenv("PERMUTIVE_BATCH_TIMEOUT_SECONDS", "42.5")
    request_env = BatchRequest(method="GET", url="https://example.com/timeout")
    assert request_env.timeout == pytest.approx(42.5)


def test_batch_request_timeout_env_validation(monkeypatch):
    """Invalid timeout values raise informative errors."""

    monkeypatch.setenv("PERMUTIVE_BATCH_TIMEOUT_SECONDS", "zero")

    with pytest.raises(ValueError, match="PERMUTIVE_BATCH_TIMEOUT_SECONDS"):
        BatchRequest(method="GET", url="https://example.com/timeout")


def test_retry_config_env_defaults(monkeypatch):
    """Retry defaults honour environment overrides when provided."""

    monkeypatch.delenv("PERMUTIVE_RETRY_MAX_RETRIES", raising=False)
    monkeypatch.delenv("PERMUTIVE_RETRY_BACKOFF_FACTOR", raising=False)
    monkeypatch.delenv("PERMUTIVE_RETRY_INITIAL_DELAY_SECONDS", raising=False)

    baseline = http.RetryConfig()
    assert baseline.max_retries == http.MAX_RETRIES
    assert baseline.backoff_factor == pytest.approx(http.BACKOFF_FACTOR)
    assert baseline.initial_delay == pytest.approx(http.INITIAL_DELAY)

    monkeypatch.setenv("PERMUTIVE_RETRY_MAX_RETRIES", "7")
    monkeypatch.setenv("PERMUTIVE_RETRY_BACKOFF_FACTOR", "3.5")
    monkeypatch.setenv("PERMUTIVE_RETRY_INITIAL_DELAY_SECONDS", "2.5")

    overridden = http.RetryConfig()
    assert overridden.max_retries == 7
    assert overridden.backoff_factor == pytest.approx(3.5)
    assert overridden.initial_delay == pytest.approx(2.5)


def test_retry_config_env_validation(monkeypatch):
    """Invalid retry environment configuration raises ValueError."""

    monkeypatch.setenv("PERMUTIVE_RETRY_MAX_RETRIES", "zero")
    with pytest.raises(ValueError, match="PERMUTIVE_RETRY_MAX_RETRIES"):
        http.RetryConfig()

    monkeypatch.setenv("PERMUTIVE_RETRY_MAX_RETRIES", "5")
    monkeypatch.setenv("PERMUTIVE_RETRY_BACKOFF_FACTOR", "nope")
    with pytest.raises(ValueError, match="PERMUTIVE_RETRY_BACKOFF_FACTOR"):
        http.RetryConfig()

    monkeypatch.setenv("PERMUTIVE_RETRY_BACKOFF_FACTOR", "3")
    monkeypatch.setenv("PERMUTIVE_RETRY_INITIAL_DELAY_SECONDS", "-1")
    with pytest.raises(ValueError, match="PERMUTIVE_RETRY_INITIAL_DELAY_SECONDS"):
        http.RetryConfig()


def test_with_retry_respects_env_defaults(monkeypatch):
    """Environment overrides control the retry loop when unspecified."""

    calls = {"n": 0}

    def method(url, headers=None, **kwargs):
        calls["n"] += 1
        resp = Response()
        resp.status_code = 503
        return resp

    monkeypatch.setattr(time, "sleep", lambda s: None)
    monkeypatch.setenv("PERMUTIVE_RETRY_MAX_RETRIES", "2")

    with pytest.raises(Exception, match="Max retries reached"):
        http._with_retry(method, "http://example.com", "api-key")

    assert calls["n"] == 2


def test_request_methods(monkeypatch):
    """Test the static and instance request methods."""

    def dummy_response(*args, **kwargs):
        r = Response()
        r.status_code = 200
        return r

    monkeypatch.setattr(http, "_with_retry", lambda *a, **k: dummy_response())
    response = http.get("k", "http://a")
    assert response is not None
    assert response.status_code == 200
    response = http.post("k", "http://a", {})
    assert response is not None
    assert response.status_code == 200
    response = http.patch("k", "http://a", {})
    assert response is not None
    assert response.status_code == 200
    response = http.delete("k", "http://a")
    assert response is not None
    assert response.status_code == 200


def test_request_merges_headers_with_default_retry(monkeypatch):
    """Ensure custom headers reach the transport when using default retry."""

    captured: Dict[str, Dict[str, str]] = {}

    def fake_get(
        url, headers=None, **kwargs
    ):  # noqa: ANN001 - signature mirrors requests.get
        captured["headers"] = headers or {}
        resp = Response()
        resp.status_code = 200
        return resp

    monkeypatch.setattr(http.requests, "get", fake_get)

    response = http.request(
        "GET",
        api_key="test-key",
        url="https://example.com/resource",
        headers={"X-Test": "value"},
    )

    assert response.status_code == 200
    assert captured["headers"]["X-Test"] == "value"
    assert captured["headers"]["Accept"] == "application/json"
    assert captured["headers"]["Content-Type"] == "application/json"


def test_to_payload():
    """Test the payload creation from a dataclass."""

    @dataclass
    class Data:
        a: int
        b: int | None = None

    result = http.to_payload(Data(1, None))
    assert result == {"a": 1}
    subset = http.to_payload(Data(1, 2), ["b"])
    assert subset == {"b": 2}


def test_with_retry(monkeypatch):
    """Test the retry mechanism for requests."""
    calls = {"n": 0}

    def method(url, headers=None, **kwargs):
        resp = Response()
        if calls["n"] == 0:
            resp.status_code = 500
            calls["n"] += 1
        else:
            resp.status_code = 200
        return resp

    monkeypatch.setattr(time, "sleep", lambda s: None)
    resp = http._with_retry(method, "http://a", "k")
    assert resp is not None
    assert resp.status_code == 200


def test_with_retry_params(monkeypatch):
    """Test that params are correctly passed through the retry mechanism."""
    calls = {"n": 0}

    def method(url, headers=None, **kwargs):
        calls["n"] += 1
        assert kwargs["params"]["test"] == "value"
        assert kwargs["params"]["k"] == "test-key"
        resp = Response()
        resp.status_code = 200
        return resp

    monkeypatch.setattr(time, "sleep", lambda s: None)
    resp = http._with_retry(method, "http://a", "test-key", params={"test": "value"})
    assert resp is not None
    assert resp.status_code == 200
    assert calls["n"] == 1


def test_with_retry_429(monkeypatch):
    """Test the retry mechanism for 429 status code with Retry-After header."""
    calls = {"n": 0}

    def method(url, headers=None, **kwargs):
        resp = Response()
        if calls["n"] == 0:
            calls["n"] += 1
            resp.status_code = 429
            resp.headers["Retry-After"] = "1"
        else:
            resp.status_code = 200
        return resp

    monkeypatch.setattr(time, "sleep", lambda s: None)
    resp = http._with_retry(method, "http://a", "k")
    assert resp is not None
    assert resp.status_code == 200
    assert calls["n"] == 1


def test_with_retry_max_retries_exceeded(monkeypatch):
    """Test that an exception is raised when max retries are exceeded."""
    calls = {"n": 0}

    def method(url, headers=None, **kwargs):
        calls["n"] += 1
        resp = Response()
        resp.status_code = 503  # Service Unavailable
        return resp

    monkeypatch.setattr(time, "sleep", lambda s: None)

    with pytest.raises(Exception, match="Max retries reached"):
        http._with_retry(method, "http://a", "k")

    assert calls["n"] == http.RetryConfig().max_retries


def test_with_retry_logs_only_after_exhaustion(monkeypatch, caplog):
    """Ensure request errors are logged once after retries with redaction."""

    calls = {"count": 0}

    def method(url, headers=None, **kwargs):
        calls["count"] += 1
        raise RequestException(
            "HTTPSConnectionPool(host='api.permutive.com', port=443): Max retries exceeded "
            "with url: /v2.0/identify?k=super-secret"
        )

    monkeypatch.setattr(time, "sleep", lambda s: None)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(PermutiveAPIError) as exc:
            http._with_retry(
                method, "https://api.permutive.com/v2.0/identify", "super-secret"
            )

    assert calls["count"] == http.RetryConfig().max_retries
    messages = [record.getMessage() for record in caplog.records]
    redacted_logs = [m for m in messages if "Request failed after" in m]
    assert len(redacted_logs) == 1
    assert "[REDACTED]" in redacted_logs[0]
    assert "super-secret" not in redacted_logs[0]
    assert "[REDACTED]" in str(exc.value)
    assert "super-secret" not in str(exc.value)


def test_json_serializable_collections():
    """Test the JSONSerializable base class for collections."""

    class DictJSON(dict, JSONSerializable[Dict[str, Any]]):
        pass

    class ListJSON(list, JSONSerializable[List[Any]]):
        pass

    class Plain(JSONSerializable[Dict[str, Any]]):
        def __init__(self):
            self.a = 1
            self._hide = 2

    class SlotNoDict:
        __slots__ = ()

    assert DictJSON({"a": 1}).to_json() == {"a": 1}
    assert ListJSON([1, None, 2]).to_json() == [1, 2]
    assert Plain().to_json() == {"a": 1}
    with pytest.raises(TypeError):
        JSONSerializable.to_json(SlotNoDict())  # type: ignore[arg-type]
