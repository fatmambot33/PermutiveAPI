"""Secret-safe actionable guidance for SDK and governed execution failures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit

from .sdk import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    DecodingError,
    NotFoundError,
    RateLimitError,
    SDKError,
    ServerError,
    TransportError,
    ValidationError,
)


@dataclass(frozen=True)
class ErrorGuidance:
    """Describe a stable failure code and the safest next action.

    Parameters
    ----------
    code
        Stable machine-readable error code.
    retryable
        Whether retrying the same operation can reasonably succeed.
    recommended_action
        Secret-free corrective action for a developer or agent.
    safe_context
        Sanitized metadata that excludes credentials, payloads, and messages.
    """

    code: str
    retryable: bool
    recommended_action: str
    safe_context: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable guidance."""
        return {
            "code": self.code,
            "retryable": self.retryable,
            "recommended_action": self.recommended_action,
            "safe_context": dict(self.safe_context),
        }


def classify_exception(
    error: BaseException,
    *,
    operation: str | None = None,
) -> ErrorGuidance:
    """Classify one failure without exposing its message or payload."""
    code, retryable, action = _classification(error)
    context: dict[str, object] = {}
    if operation:
        context["operation"] = operation
    if isinstance(error, SDKError):
        if error.status_code is not None:
            context["status_code"] = error.status_code
        if error.request_id is not None:
            context["request_id"] = error.request_id
        if error.endpoint is not None:
            context["endpoint"] = _safe_endpoint(error.endpoint)
        context["attempts"] = error.attempts
        retryable = error.retryable or retryable
    return ErrorGuidance(
        code=code,
        retryable=retryable,
        recommended_action=action,
        safe_context=context,
    )


def _safe_endpoint(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _classification(error: BaseException) -> tuple[str, bool, str]:
    if isinstance(error, AuthenticationError):
        return (
            "authentication_failed",
            False,
            "Verify the locally configured PERMUTIVE_API_KEY and run permutiveapi doctor.",
        )
    if isinstance(error, AuthorizationError):
        return (
            "authorization_denied",
            False,
            "Use credentials with permission for this workspace and operation.",
        )
    if isinstance(error, ValidationError) or isinstance(error, (TypeError, ValueError)):
        return (
            "invalid_request",
            False,
            "Correct the arguments using the published schema before retrying.",
        )
    if isinstance(error, NotFoundError):
        return (
            "resource_not_found",
            False,
            "Verify the resource identifier and active workspace.",
        )
    if isinstance(error, ConflictError):
        return (
            "resource_conflict",
            False,
            "Refresh the resource state and submit a non-conflicting change.",
        )
    if isinstance(error, RateLimitError):
        return (
            "rate_limited",
            True,
            "Retry after the server-provided delay or reduce request concurrency.",
        )
    if isinstance(error, ServerError):
        return (
            "upstream_server_error",
            True,
            "Retry with the configured bounded retry policy.",
        )
    if isinstance(error, TransportError):
        return (
            "transport_unavailable",
            True,
            "Check network reachability and retry with bounded backoff.",
        )
    if isinstance(error, DecodingError):
        return (
            "invalid_response",
            False,
            "Record the request ID and inspect upstream schema compatibility.",
        )
    if isinstance(error, PermissionError):
        return (
            "policy_denied",
            False,
            "Request the required approval or choose an allowed read-only operation.",
        )
    if isinstance(error, KeyError):
        return (
            "unknown_tool",
            False,
            "Discover the current tool registry and select a published tool name.",
        )
    return (
        "tool_execution_failed",
        False,
        "Inspect the safe error type and audit ID, then correct the operation.",
    )


__all__ = ["ErrorGuidance", "classify_exception"]
