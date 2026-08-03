"""Tests for reusable HTTP sessions."""

from __future__ import annotations

import threading
from unittest.mock import Mock

import requests
from requests.models import Response

from PermutiveAPI.utils import http


def _success_response() -> Response:
    """Return a successful mock response."""
    response = Response()
    response.status_code = 200
    return response


def test_get_session_reuses_session_in_same_thread() -> None:
    """The current thread should reuse one session until it is closed."""
    http.close_session()

    first = http.get_session()
    second = http.get_session()

    assert first is second
    http.close_session()


def test_close_session_replaces_current_thread_session() -> None:
    """Closing a session should cause the next access to create a new one."""
    http.close_session()
    first = http.get_session()

    http.close_session()
    second = http.get_session()

    assert first is not second
    http.close_session()


def test_sessions_are_isolated_between_threads() -> None:
    """Worker threads should not share mutable session instances."""
    sessions = []

    def capture_session() -> None:
        sessions.append(http.get_session())
        http.close_session()

    first_thread = threading.Thread(target=capture_session)
    second_thread = threading.Thread(target=capture_session)
    first_thread.start()
    second_thread.start()
    first_thread.join()
    second_thread.join()

    assert len(sessions) == 2
    assert sessions[0] is not sessions[1]


def test_request_uses_injected_session() -> None:
    """An explicitly injected session should handle the request."""
    session = Mock(spec=requests.Session)
    response = _success_response()
    session.request.return_value = response

    result = http.request(
        method="GET",
        api_key="secret",
        url="https://api.example.com/items",
        params={"page": 2},
        session=session,
    )

    assert result is response
    session.request.assert_called_once_with(
        method="GET",
        url="https://api.example.com/items",
        headers=http.DEFAULT_HEADERS,
        params={"page": 2, "k": "secret"},
        timeout=10.0,
    )


def test_get_helper_uses_injected_session() -> None:
    """Convenience helpers should also support explicit session injection."""
    session = Mock(spec=requests.Session)
    response = _success_response()
    session.get.return_value = response

    result = http.get(
        api_key="secret",
        url="https://api.example.com/items",
        session=session,
    )

    assert result is response
    session.get.assert_called_once_with(
        "https://api.example.com/items",
        headers=http.DEFAULT_HEADERS,
        params={"k": "secret"},
    )
