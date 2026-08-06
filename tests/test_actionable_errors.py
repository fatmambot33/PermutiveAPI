"""Tests for secret-safe actionable failure guidance."""

from __future__ import annotations

import json

import pytest

from PermutiveAPI.actionable_errors import classify_exception
from PermutiveAPI.ai_native import (
    GovernedToolExecutor,
    InvocationContext,
)
from PermutiveAPI.sdk import (
    AuthenticationError,
    RateLimitError,
    ServerError,
)
from PermutiveAPI.tools import ToolDefinition, ToolRegistry


@pytest.mark.parametrize(
    ("error", "code", "retryable"),
    (
        (AuthenticationError("secret"), "authentication_failed", False),
        (
            RateLimitError("secret", status_code=429, retryable=True),
            "rate_limited",
            True,
        ),
        (
            ServerError("secret", status_code=503, retryable=True),
            "upstream_server_error",
            True,
        ),
        (ValueError("secret"), "invalid_request", False),
        (PermissionError("secret"), "policy_denied", False),
        (KeyError("secret"), "unknown_tool", False),
    ),
)
def test_classification_is_stable_and_secret_free(
    error: BaseException,
    code: str,
    retryable: bool,
) -> None:
    """Error guidance uses stable codes without raw messages."""
    guidance = classify_exception(error, operation="demo")
    serialized = json.dumps(guidance.to_dict())

    assert guidance.code == code
    assert guidance.retryable is retryable
    assert guidance.recommended_action
    assert guidance.safe_context["operation"] == "demo"
    assert "secret" not in serialized


def test_sdk_context_is_sanitized_and_actionable() -> None:
    """Safe SDK metadata survives while messages and payloads do not."""
    error = RateLimitError(
        "token=secret",
        status_code=429,
        request_id="request-123",
        endpoint="https://api.example.test/v1/cohorts?token=query-secret",
        retryable=True,
        attempts=3,
    )
    guidance = classify_exception(error, operation="list_cohorts")

    assert guidance.safe_context == {
        "operation": "list_cohorts",
        "status_code": 429,
        "request_id": "request-123",
        "endpoint": "https://api.example.test/v1/cohorts",
        "attempts": 3,
    }
    serialized = json.dumps(guidance.to_dict())
    assert "secret" not in serialized
    assert "token" not in serialized


def test_governed_result_preserves_legacy_fields_and_adds_guidance() -> None:
    """Invocation failures remain compatible while adding next-action metadata."""

    def fail() -> None:
        raise RateLimitError(
            "payload-secret",
            status_code=429,
            retryable=True,
        )

    executor = GovernedToolExecutor(
        ToolRegistry(
            (
                ToolDefinition(
                    "rate_limited_tool",
                    "Fail with a deterministic rate limit.",
                    {"type": "object", "properties": {}},
                    fail,
                ),
            )
        )
    )
    result = executor.invoke(
        "rate_limited_tool",
        {},
        context=InvocationContext(run_id="actionable-error"),
    )

    assert result.ok is False
    assert result.error_type == "RateLimitError"
    assert result.error_message == "Tool execution failed with RateLimitError."
    assert result.error_code == "rate_limited"
    assert result.retryable is True
    assert result.recommended_action
    assert result.safe_context["operation"] == "rate_limited_tool"
    assert "payload-secret" not in json.dumps(result.to_dict())
