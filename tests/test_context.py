"""Tests for the Context module."""

from types import SimpleNamespace
from unittest.mock import Mock

from PermutiveAPI.Context import ContextSegment


def test_context_segment_to_json():
    """Verify that ``to_json`` produces the expected payload."""
    request = ContextSegment(
        url="https://example.com/article/sports-news",
        page_properties={
            "client": {
                "url": "https://example.com/article/sports-news",
                "domain": "example.com",
                "referrer": "https://example.com",
                "type": "web",
                "user_agent": "Mozilla/5.0",
                "title": "Latest Sports News",
            },
            "category": "sports",
            "tags": ["football", "premier-league"],
        },
    )

    expected_payload = {
        "url": "https://example.com/article/sports-news",
        "page_properties": {
            "client": {
                "url": "https://example.com/article/sports-news",
                "domain": "example.com",
                "referrer": "https://example.com",
                "type": "web",
                "user_agent": "Mozilla/5.0",
                "title": "Latest Sports News",
            },
            "category": "sports",
            "tags": ["football", "premier-league"],
        },
    }

    assert request.to_json() == expected_payload


def test_context_segment_send(monkeypatch):
    """Verify that ``send`` calls the expected endpoint and returns JSON."""
    mock_response = Mock()
    mock_response.json.return_value = {"segments": [{"id": "seg-1"}]}

    mock_request = Mock(return_value=mock_response)

    monkeypatch.setattr(
        ContextSegment,
        "_request_helper",
        SimpleNamespace(request=mock_request),
    )

    request = ContextSegment(
        url="https://example.com/article/sports-news",
        page_properties={"category": "sports"},
    )

    response = request.send(api_key="test-key", timeout=5.0)

    assert response == {"segments": [{"id": "seg-1"}]}
    mock_request.assert_called_once_with(
        method="POST",
        api_key="test-key",
        url="https://api.permutive.com/ctx/v1/segment",
        json={
            "url": "https://example.com/article/sports-news",
            "page_properties": {"category": "sports"},
        },
        timeout=5.0,
    )
