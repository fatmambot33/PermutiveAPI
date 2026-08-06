"""Tests for the supported package-root API contract."""

from __future__ import annotations

from importlib import import_module

import PermutiveAPI

EXPECTED_PUBLIC_API = {
    "API_CONTRACT_VERSION",
    "Alias",
    "AliasPayload",
    "ArtifactDigest",
    "AsyncPermutiveClient",
    "AsyncResource",
    "AsyncResponse",
    "AsyncResponseLike",
    "AsyncTransport",
    "AsyncTransportLike",
    "AtomicCredentials",
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
    "CoordinatedAsyncTransport",
    "CoordinatedTransport",
    "CredentialSnapshot",
    "DecodingError",
    "DriftKind",
    "EndpointContract",
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
    "PERFORMANCE_CONTRACT_VERSION",
    "PERMUTIVE_MCP_DOCUMENTATION_URL",
    "PERMUTIVE_MCP_SERVER_NAME",
    "PERMUTIVE_MCP_TOKEN_ENV",
    "PERMUTIVE_MCP_URL_ENV",
    "Page",
    "PerformanceBudget",
    "PerformanceResult",
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
    "RECORDING_FORMAT_VERSION",
    "RELEASE_EVIDENCE_VERSION",
    "RateLimitCoordinator",
    "RateLimitError",
    "Recipe",
    "RecipeCategory",
    "RecordedInteraction",
    "Recording",
    "RecordingTransport",
    "ReplayMismatchError",
    "ReplayTransport",
    "Resource",
    "ResponseKind",
    "RetryPolicy",
    "SDKError",
    "SchemaDrift",
    "SchemaDriftError",
    "Secret",
    "Segment",
    "SegmentList",
    "Segmentation",
    "SegmentationPayload",
    "ServerError",
    "Source",
    "SyncTransport",
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
    "classify_response_schema",
    "contract_manifest",
    "create_release_manifest",
    "digest_file",
    "endpoint_contract",
    "endpoint_contracts",
    "event",
    "execute_async_batch",
    "execute_batch",
    "find_recipes",
    "first_success_contract",
    "in_segment",
    "load_performance_budgets",
    "measure_first_success",
    "measure_operation",
    "negotiate_capabilities",
    "performance_report",
    "property_condition",
    "recipe_catalog",
    "recipe_manifest",
    "sanitize_json",
    "schema_fingerprint",
    "structural_schema",
    "tool",
    "validate_operation_names",
    "validate_response_schema",
    "verify_release_manifest",
    "write_release_manifest",
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
