"""Tests for the supported package-root API contract."""

from __future__ import annotations

from importlib import import_module

import PermutiveAPI

EXPECTED_PUBLIC_API = {
    "Alias",
    "AliasPayload",
    "AsyncPermutiveClient",
    "AsyncResponse",
    "AsyncTransport",
    "AuthenticationError",
    "AuthorizationError",
    "BatchItem",
    "BatchResult",
    "Cohort",
    "CohortList",
    "ConflictError",
    "ContextPayload",
    "ContextSegment",
    "DecodingError",
    "Event",
    "EventPayload",
    "Identity",
    "IdentityPayload",
    "Import",
    "ImportList",
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
    "PermutiveAPIError",
    "PermutiveAgentKit",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveClient",
    "PermutiveMCPConfig",
    "PermutiveRateLimitError",
    "PermutiveResourceNotFoundError",
    "PermutiveServerError",
    "RateLimitError",
    "Resource",
    "RetryPolicy",
    "SDKError",
    "Segment",
    "SegmentList",
    "Segmentation",
    "SegmentationPayload",
    "ServerError",
    "Source",
    "ToolDefinition",
    "ToolHandler",
    "ToolRegistry",
    "TransportError",
    "ValidationError",
    "Workspace",
    "WorkspaceList",
    "execute_batch",
    "tool",
}


def test_public_api_is_explicit_and_complete() -> None:
    """Ensure the stable package-root API is intentional and importable."""
    assert set(PermutiveAPI.__all__) == EXPECTED_PUBLIC_API

    for public_name in PermutiveAPI.__all__:
        assert hasattr(PermutiveAPI, public_name), public_name


def test_public_api_supports_explicit_package_root_imports() -> None:
    """Ensure every documented symbol supports a direct root import."""
    package = import_module("PermutiveAPI")

    for public_name in EXPECTED_PUBLIC_API:
        namespace: dict[str, object] = {}
        exec(f"from PermutiveAPI import {public_name}", namespace)
        assert namespace[public_name] is getattr(package, public_name)


def test_public_api_does_not_contain_duplicates() -> None:
    """Ensure duplicate exports cannot hide mistakes in the API inventory."""
    assert len(PermutiveAPI.__all__) == len(set(PermutiveAPI.__all__))
