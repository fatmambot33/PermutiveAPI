"""Tests for the supported package-root API contract."""

from __future__ import annotations

from importlib import import_module

import PermutiveAPI

EXPECTED_PUBLIC_API = {
    "Alias",
    "AliasPayload",
    "AsyncPermutiveClient",
    "AsyncResource",
    "AsyncResponse",
    "AsyncTransport",
    "AuthenticationError",
    "AuthorizationError",
    "BatchItem",
    "BatchResult",
    "CAPABILITY_CONTRACT_VERSION",
    "CAPABILITY_ERROR_CODES",
    "CapabilityDescriptor",
    "CapabilityNegotiationError",
    "CapabilityRequirement",
    "Cohort",
    "CohortList",
    "ConflictError",
    "ContextPayload",
    "ContextSegment",
    "DecodingError",
    "ErrorGuidance",
    "Event",
    "EventPayload",
    "FIRST_SUCCESS_BUDGET_SECONDS",
    "FIRST_SUCCESS_CONTRACT_VERSION",
    "FIRST_SUCCESS_RECIPE",
    "FirstSuccessMeasurement",
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
    "PermutiveConfig",
    "PermutiveMCPConfig",
    "PermutiveRateLimitError",
    "PermutiveResourceNotFoundError",
    "PermutiveServerError",
    "QueryExpression",
    "RateLimitError",
    "Recipe",
    "RecipeCategory",
    "Resource",
    "RetryPolicy",
    "SDKError",
    "Secret",
    "Segment",
    "SegmentList",
    "Segmentation",
    "SegmentationPayload",
    "ServerError",
    "Source",
    "TOOL_SCHEMA_VERSION",
    "ToolDefinition",
    "ToolHandler",
    "ToolRegistry",
    "TransportError",
    "ValidationError",
    "Workspace",
    "WorkspaceList",
    "all_of",
    "any_of",
    "capability_contract_manifest",
    "classify_exception",
    "event",
    "execute_async_batch",
    "execute_batch",
    "find_recipes",
    "first_success_contract",
    "in_segment",
    "measure_first_success",
    "negotiate_capabilities",
    "property_condition",
    "recipe_catalog",
    "recipe_manifest",
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
