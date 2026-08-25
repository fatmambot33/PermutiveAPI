"""Generated classification for the package-root public SDK surface."""

from __future__ import annotations

from typing import Iterable, Tuple

PUBLIC_SURFACE_VERSION = 1

_COMPATIBILITY_EXPORTS = frozenset(
    {
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
)

_INTEGRATION_EXPORTS = frozenset(
    {
        "CAPABILITY_CONTRACT_VERSION",
        "CAPABILITY_ERROR_CODES",
        "TOOL_SCHEMA_VERSION",
        "CapabilityDescriptor",
        "CapabilityNegotiationError",
        "CapabilityRequirement",
        "JSONSchema",
        "PERMUTIVE_MCP_DOCUMENTATION_URL",
        "PERMUTIVE_MCP_SERVER_NAME",
        "PERMUTIVE_MCP_TOKEN_ENV",
        "PERMUTIVE_MCP_URL_ENV",
        "PermutiveAgentKit",
        "PermutiveMCPConfig",
        "ToolDefinition",
        "ToolHandler",
        "ToolRegistry",
        "capability_contract_manifest",
        "negotiate_capabilities",
        "tool",
    }
)

_CANONICAL_EXPORTS = frozenset(
    {
        "API_CONTRACT_VERSION",
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
        "ConflictError",
        "ContextPayload",
        "CoordinatedAsyncTransport",
        "CoordinatedTransport",
        "CredentialSnapshot",
        "DecodingError",
        "DriftKind",
        "EndpointContract",
        "ErrorGuidance",
        "EventPayload",
        "FIRST_SUCCESS_BUDGET_SECONDS",
        "FIRST_SUCCESS_CONTRACT_VERSION",
        "FIRST_SUCCESS_RECIPE",
        "FirstSuccessMeasurement",
        "IdentityPayload",
        "JSONObject",
        "JSONScalar",
        "JSONValue",
        "NotFoundError",
        "PERFORMANCE_CONTRACT_VERSION",
        "Page",
        "PerformanceBudget",
        "PerformanceResult",
        "PermutiveClient",
        "PermutiveConfig",
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
        "SegmentationPayload",
        "ServerError",
        "SyncTransport",
        "TransportError",
        "ValidationError",
        "all_of",
        "any_of",
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
        "performance_report",
        "property_condition",
        "recipe_catalog",
        "recipe_manifest",
        "sanitize_json",
        "schema_fingerprint",
        "structural_schema",
        "validate_operation_names",
        "validate_response_schema",
        "verify_release_manifest",
        "write_release_manifest",
    }
)

_CLASSIFICATIONS = {
    **{name: "canonical" for name in _CANONICAL_EXPORTS},
    **{name: "integration" for name in _INTEGRATION_EXPORTS},
    **{name: "compatibility" for name in _COMPATIBILITY_EXPORTS},
}

if len(_CLASSIFICATIONS) != (
    len(_CANONICAL_EXPORTS) + len(_INTEGRATION_EXPORTS) + len(_COMPATIBILITY_EXPORTS)
):
    raise RuntimeError("Public export classifications must not overlap.")


def classify_public_export(name: str) -> str:
    """Return the explicit stable classification for one package-root export."""
    try:
        return _CLASSIFICATIONS[name]
    except KeyError as error:
        raise ValueError(f"Unclassified package-root export: {name}") from error


def public_surface_manifest(exports: Iterable[str]) -> dict[str, object]:
    """Build deterministic package-root public API evidence."""
    values: Tuple[str, ...] = tuple(exports)
    if len(values) != len(set(values)):
        raise ValueError("Package-root exports must not contain duplicates.")
    unknown = set(values) - set(_CLASSIFICATIONS)
    missing = set(_CLASSIFICATIONS) - set(values)
    if unknown:
        raise ValueError("Unclassified package-root exports: " + ", ".join(sorted(unknown)))
    if missing:
        raise ValueError("Classified exports missing from package root: " + ", ".join(sorted(missing)))
    classified = {
        category: sorted(
            name for name in values if classify_public_export(name) == category
        )
        for category in ("canonical", "integration", "compatibility")
    }
    return {
        "version": PUBLIC_SURFACE_VERSION,
        "canonical_import": "PermutiveAPI",
        "export_count": len(values),
        "classifications": classified,
    }


__all__ = [
    "PUBLIC_SURFACE_VERSION",
    "classify_public_export",
    "public_surface_manifest",
]
