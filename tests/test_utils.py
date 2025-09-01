from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import uuid
import urllib.parse
import time
from typing import Dict, Any, List

import pytest
from PermutiveAPI.errors import (
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
)
from requests.models import PreparedRequest, Response
from requests.structures import CaseInsensitiveDict

from PermutiveAPI.Utils import (
    RequestHelper,
    JSONSerializable,
    check_filepath,
    split_filepath,
    chunk_list,
    convert_list,
    compare_list,
    merge_list,
    json_default,
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


def test_generate_url_with_key():
    """Test that the API key is correctly added to a URL."""
    url = "https://api.com/resource?existing=1"
    result = RequestHelper.generate_url_with_key(url, "abc")
    parsed = urllib.parse.urlparse(result)
    query = urllib.parse.parse_qs(parsed.query)
    assert query["k"] == ["abc"]
    assert query["existing"] == ["1"]


def test_redact_sensitive_data():
    """Test redaction of sensitive data from URLs and messages."""
    # Test redacting from URL query parameters
    url_with_key = "https://api.com/resource?api_key=secret_key&other=val"
    resp = _make_response(url_with_key, 400)

    # Test message containing sensitive key-value pairs
    message_with_secrets = 'some info api_key=secret_key and token="secret_token"'

    redacted_message = RequestHelper._redact_sensitive_data(message_with_secrets, resp)
    assert "secret_key" not in redacted_message
    assert "secret_token" not in redacted_message
    assert "[REDACTED]" in redacted_message

    # Test with no sensitive data
    url_without_key = "https://api.com/resource?other=val"
    resp_no_secret = _make_response(url_without_key, 200)
    message_without_secrets = "this is fine"
    redacted_message = RequestHelper._redact_sensitive_data(
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
    assert RequestHelper._extract_error_message(resp_valid) == "bad request"

    # Test malformed JSON error
    resp_malformed = _make_response("https://api.com", 400, b'{"error": "cause"}')
    assert RequestHelper._extract_error_message(resp_malformed) == "Unknown error"

    # Test non-JSON error
    resp_not_json = _make_response("https://api.com", 400, b"not json")
    assert (
        RequestHelper._extract_error_message(resp_not_json)
        == "Could not parse error message"
    )


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
        RequestHelper.handle_exception(Exception("boom"), resp)

    # For 400, check that the message contains the URL and that it's redacted
    if status == 400:
        assert "bad request" in str(excinfo.value)
        assert "secret" not in str(excinfo.value)
        assert "[REDACTED]" in str(excinfo.value)


def test_handle_exception_no_response():
    """Test that a generic error is raised when there is no response."""
    with pytest.raises(PermutiveAPIError, match="An unexpected error occurred"):
        RequestHelper.handle_exception(Exception("boom"), None)


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
    check_filepath(str(file_path))
    assert dir_path.exists()

    # Test split_filepath
    path, name, ext = split_filepath(str(file_path))
    assert path == str(dir_path)
    assert name == "test"
    assert ext == ".txt"


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
    dummy.to_json_file(str(path))
    assert Dummy.from_json_file(str(path)) == dummy
    assert str(dummy).startswith("{")

    json_str = json.dumps(dummy.to_json())
    assert Dummy.from_json(json_str) == dummy
    assert Dummy.from_json(path) == dummy
    assert Dummy.from_json({"id": 1, "name": "a", "values": [1, 2]}) == dummy

    with pytest.raises(TypeError):
        Dummy.from_json(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Dummy.from_json("not json")


def test_request_methods(monkeypatch):
    """Test the static and instance request methods."""

    def dummy_response(*args, **kwargs):
        r = Response()
        r.status_code = 200
        return r

    monkeypatch.setattr(RequestHelper, "_with_retry", lambda *a, **k: dummy_response())
    response = RequestHelper.get_static("k", "http://a")
    assert response is not None
    assert response.status_code == 200
    response = RequestHelper.post_static("k", "http://a", {})
    assert response is not None
    assert response.status_code == 200
    response = RequestHelper.patch_static("k", "http://a", {})
    assert response is not None
    assert response.status_code == 200
    response = RequestHelper.delete_static("k", "http://a")
    assert response is not None
    assert response.status_code == 200

    helper = RequestHelper("k", "http://a")
    monkeypatch.setattr(
        RequestHelper, "get_static", lambda api_key, url: dummy_response()
    )
    monkeypatch.setattr(
        RequestHelper, "post_static", lambda api_key, url, data: dummy_response()
    )
    monkeypatch.setattr(
        RequestHelper, "patch_static", lambda api_key, url, data: dummy_response()
    )
    monkeypatch.setattr(
        RequestHelper, "delete_static", lambda api_key, url: dummy_response()
    )
    response = helper.get("u")
    assert response is not None
    assert response.status_code == 200
    response = helper.post("u", {})
    assert response is not None
    assert response.status_code == 200
    response = helper.patch("u", {})
    assert response is not None
    assert response.status_code == 200
    response = helper.delete("u")
    assert response is not None
    assert response.status_code == 200


def test_to_payload_static():
    """Test the payload creation from a dataclass."""

    @dataclass
    class Data:
        a: int
        b: int | None = None

    result = RequestHelper.to_payload_static(Data(1, None))
    assert result == {"a": 1}
    subset = RequestHelper.to_payload_static(Data(1, 2), ["b"])
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
    resp = RequestHelper._with_retry(method, "http://a", "k")
    assert resp is not None
    assert resp.status_code == 200


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
    resp = RequestHelper._with_retry(method, "http://a", "k")
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
        RequestHelper._with_retry(method, "http://a", "k")

    assert calls["n"] == RequestHelper.MAX_RETRIES


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
