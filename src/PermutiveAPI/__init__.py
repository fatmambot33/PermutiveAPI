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
]
