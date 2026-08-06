"""Public imports for the typed Permutive API SDK."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .actionable_errors import ErrorGuidance, classify_exception
from .agent import PermutiveAgentKit
from .async_client import (
    AsyncPermutiveClient,
    AsyncResource,
    AsyncResponse,
    AsyncTransport,
    execute_async_batch,
)
from .capabilities import (
    CAPABILITY_CONTRACT_VERSION,
    CAPABILITY_ERROR_CODES,
    TOOL_SCHEMA_VERSION,
    CapabilityDescriptor,
    CapabilityNegotiationError,
    CapabilityRequirement,
    capability_contract_manifest,
    negotiate_capabilities,
)
from .client import PermutiveClient
from .config import PermutiveConfig, Secret
from .contracts import (
    API_CONTRACT_VERSION,
    DriftKind,
    EndpointContract,
    ResponseKind,
    SchemaDrift,
    SchemaDriftError,
    classify_response_schema,
    contract_manifest,
    endpoint_contract,
    endpoint_contracts,
    schema_fingerprint,
    structural_schema,
    validate_response_schema,
)
from .first_success import (
    FIRST_SUCCESS_BUDGET_SECONDS,
    FIRST_SUCCESS_CONTRACT_VERSION,
    FIRST_SUCCESS_RECIPE,
    FirstSuccessMeasurement,
    first_success_contract,
    measure_first_success,
)
from .mcp import (
    PERMUTIVE_MCP_DOCUMENTATION_URL,
    PERMUTIVE_MCP_SERVER_NAME,
    PERMUTIVE_MCP_TOKEN_ENV,
    PERMUTIVE_MCP_URL_ENV,
    PermutiveMCPConfig,
)
from .models import (
    AliasPayload,
    ContextPayload,
    EventPayload,
    IdentityPayload,
    SegmentationPayload,
)
from .performance import (
    PERFORMANCE_CONTRACT_VERSION,
    PerformanceBudget,
    PerformanceResult,
    load_performance_budgets,
    measure_operation,
    performance_report,
    validate_operation_names,
)
from .query_dsl import (
    QueryExpression,
    all_of,
    any_of,
    event,
    in_segment,
    property_condition,
)
from .recipes import (
    Recipe,
    RecipeCategory,
    find_recipes,
    recipe_catalog,
    recipe_manifest,
)
from .recording import (
    RECORDING_FORMAT_VERSION,
    RecordedInteraction,
    Recording,
    RecordingTransport,
    ReplayMismatchError,
    ReplayTransport,
    sanitize_json,
)
from .release_evidence import (
    RELEASE_EVIDENCE_VERSION,
    ArtifactDigest,
    create_release_manifest,
    digest_file,
    verify_release_manifest,
    write_release_manifest,
)
from .resilience import (
    AsyncResponseLike,
    AsyncTransportLike,
    AtomicCredentials,
    CoordinatedAsyncTransport,
    CoordinatedTransport,
    CredentialSnapshot,
    RateLimitCoordinator,
    SyncTransport,
)
from .resources import Resource
from .sdk import (
    AuthenticationError,
    AuthorizationError,
    BatchItem,
    BatchResult,
    ConflictError,
    DecodingError,
    JSONObject,
    JSONScalar,
    JSONValue,
    NotFoundError,
    Page,
    RateLimitError,
    RetryPolicy,
    SDKError,
    ServerError,
    TransportError,
    ValidationError,
    execute_batch,
)
from .tools import JSONSchema, ToolDefinition, ToolHandler, ToolRegistry, tool
from .utils.http import (
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
)

if TYPE_CHECKING:
    from .audience import Import, ImportList, Segment, SegmentList, Source
    from .cohort import Cohort, CohortList
    from .context import ContextSegment
    from .identify import Alias, Identity
    from .segmentation import Event, Segmentation
    from .workspace import Workspace, WorkspaceList

_LAZY_EXPORTS = {
    "Alias": ("PermutiveAPI.identify", "Alias"),
    "Cohort": ("PermutiveAPI.cohort", "Cohort"),
    "CohortList": ("PermutiveAPI.cohort", "CohortList"),
    "ContextSegment": ("PermutiveAPI.context", "ContextSegment"),
    "Event": ("PermutiveAPI.segmentation", "Event"),
    "Identity": ("PermutiveAPI.identify", "Identity"),
    "Import": ("PermutiveAPI.audience", "Import"),
    "ImportList": ("PermutiveAPI.audience", "ImportList"),
    "Segment": ("PermutiveAPI.audience", "Segment"),
    "SegmentList": ("PermutiveAPI.audience", "SegmentList"),
    "Segmentation": ("PermutiveAPI.segmentation", "Segmentation"),
    "Source": ("PermutiveAPI.audience", "Source"),
    "Workspace": ("PermutiveAPI.workspace", "Workspace"),
    "WorkspaceList": ("PermutiveAPI.workspace", "WorkspaceList"),
}


def __getattr__(name: str) -> Any:
    """Load legacy resource exports only when requested."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute_name = target
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


__all__ = [
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
]
