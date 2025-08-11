from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import uuid
import urllib.parse
import time

import pytest
from requests.models import PreparedRequest, Response

from PermutiveAPI.Utils import (
    RequestHelper,
    JSONSerializable,
    check_filepath,
    split_filepath,
    file_exists,
    chunk_list,
    convert_list,
    compare_list,
    merge_list,
    json_default,
)


class Color(Enum):
    RED = "red"


@dataclass
class Dummy(JSONSerializable):
    id: int
    name: str
    values: list[int] | None = None


class MockResponse(Response):
    def __bool__(self) -> bool:  # pragma: no cover
        return True


def _make_response(
    url: str, status: int, content: bytes = b"", headers: dict | None = None
) -> Response:
    req = PreparedRequest()
    req.prepare_url(url, None)
    resp = MockResponse()
    resp.status_code = status
    resp._content = content
    resp.request = req
    resp.headers = headers or {}
    return resp


def test_generate_url_with_key():
    url = "https://api.com/resource?existing=1"
    result = RequestHelper.generate_url_with_key(url, "abc")
    parsed = urllib.parse.urlparse(result)
    query = urllib.parse.parse_qs(parsed.query)
    assert query["k"] == ["abc"]
    assert query["existing"] == ["1"]


def test_redact_and_extract_error():
    resp = _make_response(
        "https://api.com?api_key=secret&token=t", 400, b"{'error':{'cause':'bad'}}"
    )
    msg = "api_key=secret token=t"
    redacted = RequestHelper._redact_sensitive_data(msg, resp)
    assert "secret" not in redacted and "[REDACTED]" in redacted
    assert RequestHelper._extract_error_message(resp) == "Could not parse error message"


def test_handle_exception():
    resp200 = _make_response("https://api.com", 200)
    assert RequestHelper.handle_exception(Exception("boom"), resp200) is resp200

    resp400 = _make_response(
        "https://api.com?api_key=secret", 400, b'{"error":{"cause":"bad"}}'
    )
    assert RequestHelper.handle_exception(Exception("boom"), resp400) is resp400

    resp401 = _make_response("https://api.com", 401)
    assert RequestHelper.handle_exception(Exception("boom"), resp401) is None

    resp403 = _make_response("https://api.com", 403)
    assert RequestHelper.handle_exception(Exception("boom"), resp403) is None

    resp404 = _make_response("https://api.com", 404)
    assert RequestHelper.handle_exception(Exception("boom"), resp404) is None

    resp429 = _make_response("https://api.com", 429)
    assert RequestHelper.handle_exception(Exception("boom"), resp429) is None

    resp500 = _make_response("https://api.com", 500)
    assert RequestHelper.handle_exception(Exception("boom"), resp500) is None

    with pytest.raises(Exception):
        RequestHelper.handle_exception(Exception("boom"), None)


def test_filepath_helpers(tmp_path):
    path = tmp_path / "dir" / "file.txt"
    check_filepath(str(path))
    assert path.parent.exists()

    p, name, ext = split_filepath(str(path))
    assert p.endswith("dir/")
    assert name == "file" and ext == ".txt"

    assert not file_exists(str(path))
    path.write_text("x")
    assert file_exists(str(path))
    variant = path.with_name("file-123.txt")
    variant.write_text("y")
    assert file_exists(str(path))


def test_list_helpers():
    assert chunk_list([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert convert_list("[1, 2]") == [1, 2]
    assert convert_list([1, 2]) == [1, 2]
    assert compare_list(["a", "b"], ["b", "a"]) is True
    assert merge_list([1, 2], [2, 3]) == [1, 2, 3]
    assert merge_list([1], 2) == [1, 2]
    assert merge_list([1], None) == [1]


def test_json_default():
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
        Dummy.from_json(123)
    with pytest.raises(TypeError):
        Dummy.from_json("not json")


def test_request_methods(monkeypatch):
    def dummy_response(*args, **kwargs):
        r = Response()
        r.status_code = 200
        return r

    monkeypatch.setattr(RequestHelper, "_with_retry", lambda *a, **k: dummy_response())
    assert RequestHelper.get_static("k", "http://a").status_code == 200
    assert RequestHelper.post_static("k", "http://a", {}).status_code == 200
    assert RequestHelper.patch_static("k", "http://a", {}).status_code == 200
    assert RequestHelper.delete_static("k", "http://a").status_code == 200

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
    assert helper.get("u").status_code == 200
    assert helper.post("u", {}).status_code == 200
    assert helper.patch("u", {}).status_code == 200
    assert helper.delete("u").status_code == 200


def test_to_payload_static():
    @dataclass
    class Data:
        a: int
        b: int | None = None

    result = RequestHelper.to_payload_static(Data(1, None))
    assert result == {"a": 1}
    subset = RequestHelper.to_payload_static(Data(1, 2), ["b"])
    assert subset == {"b": 2}


def test_with_retry(monkeypatch):
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
    assert resp.status_code == 200


def test_json_serializable_collections():
    class DictJSON(dict, JSONSerializable):
        pass

    class ListJSON(list, JSONSerializable):
        pass

    class Plain(JSONSerializable):
        def __init__(self):
            self.a = 1
            self._hide = 2

    class SlotNoDict:
        __slots__ = ()

    assert DictJSON({"a": 1}).to_json() == {"a": 1}
    assert ListJSON([1, None, 2]).to_json() == [1, 2]
    assert Plain().to_json() == {"a": 1}
    with pytest.raises(TypeError):
        JSONSerializable.to_json(SlotNoDict())
