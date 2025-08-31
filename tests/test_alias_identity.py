import pytest
from requests.exceptions import RequestException
from typing import cast, Dict, Any

from PermutiveAPI.Identify.Alias import Alias
from PermutiveAPI.Identify.Identity import Identity


def test_alias_serialization():
    """Test that Alias objects can be serialized and deserialized."""
    alias = Alias(id="a1", tag="email", priority=1)
    json_data = alias.to_json()
    assert json_data == {"id": "a1", "tag": "email", "priority": 1}
    alias2 = Alias.from_json(cast(Dict[str, Any], json_data))
    assert alias2 == alias


def test_identity_serialization():
    """Test that Identity objects can be serialized and deserialized."""
    aliases = [Alias(id="a1", tag="email", priority=1)]
    identity = Identity(user_id="user123", aliases=aliases)
    json_data = identity.to_json()
    json_data_dict = cast(Dict[str, Any], json_data)
    assert json_data_dict["user_id"] == "user123"
    assert json_data_dict["aliases"][0]["id"] == "a1"
    identity2 = Identity.from_json({"user_id": "user123", "aliases": aliases})
    assert identity2 == identity


def test_identify_raises_value_error(monkeypatch):
    """Test that identify raises ValueError on a None response."""
    identity = Identity(user_id="user123", aliases=[])

    def fake_post(api_key: str, url: str, data: dict):  # pragma: no cover - test stub
        return None

    monkeypatch.setattr(
        "PermutiveAPI.Identify.Identity.RequestHelper.post_static", fake_post
    )

    with pytest.raises(ValueError):
        identity.identify("api-key")


def test_identify_propagates_exception(monkeypatch):
    """Test that exceptions from the request helper are propagated."""
    identity = Identity(user_id="user123", aliases=[])

    def fake_post(api_key: str, url: str, data: dict):  # pragma: no cover - test stub
        raise RequestException("boom")

    monkeypatch.setattr(
        "PermutiveAPI.Identify.Identity.RequestHelper.post_static", fake_post
    )

    with pytest.raises(RequestException):
        identity.identify("api-key")
