"""Tests for the supported package-root API contract."""

from __future__ import annotations

import PermutiveAPI


CANONICAL_EXPORTS = {
    "AliasPayload",
    "AsyncPermutiveClient",
    "AsyncResponse",
    "AsyncTransport",
    "AuthenticationError",
    "AuthorizationError",
    "BatchItem",
    "BatchResult",
    "ConflictError",
    "ContextPayload",
    "DecodingError",
    "EventPayload",
    "IdentityPayload",
    "JSONObject",
    "JSONScalar",
    "JSONSchema",
    "JSONValue",
    "NotFoundError",
    "PERMUTIVE_MCP_DOCUMENTATION_URL",
    "PERMUTIVE_MCP_SERVER_NAME",
    "PERMUTIVE_MCP_TOKEN_ENV",
    "PERMUTIVE_MCP_URL_ENV",
    "Page",
    "PermutiveAgentKit",
    "PermutiveClient",
    "PermutiveConfig",
    "PermutiveMCPConfig",
    "QueryExpression",
    "RateLimitError",
    "Resource",
    "RetryPolicy",
    "SDKError",
    "Secret",
    "SegmentationPayload",
    "ServerError",
    "ToolDefinition",
    "ToolHandler",
    "ToolRegistry",
    "TransportError",
    "ValidationError",
    "all_of",
    "any_of",
    "event",
    "execute_batch",
    "in_segment",
    "property_condition",
    "tool",
}

COMPATIBILITY_EXPORTS = {
    "Alias",
    "Cohort",
    "CohortList",
    "ContextSegment",
    "Event",
    "Identity",
    "Import",
    "ImportList",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveRateLimitError",
    "PermutiveResourceNotFoundError",
    "PermutiveServerError",
    "Segment",
    "SegmentList",
    "Segmentation",
    "Source",
    "Workspace",
    "WorkspaceList",
}


def test_public_exports_are_fully_classified() -> None:
    """Require every package-root export to have an explicit support class."""
    classified = CANONICAL_EXPORTS | COMPATIBILITY_EXPORTS

    assert set(PermutiveAPI.__all__) == classified


def test_public_exports_are_available() -> None:
    """Require every declared public export to resolve from the package root."""
    for name in PermutiveAPI.__all__:
        assert getattr(PermutiveAPI, name) is not None


def test_public_export_names_are_unique() -> None:
    """Keep the package-root contract deterministic and unambiguous."""
    assert len(PermutiveAPI.__all__) == len(set(PermutiveAPI.__all__))
