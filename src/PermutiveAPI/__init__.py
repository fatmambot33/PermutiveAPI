"""Public imports for the typed Permutive API SDK."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from .client import PermutiveClient
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
    "JSONValue",
    "NotFoundError",
    "PERMUTIVE_MCP_DOCUMENTATION_URL",
    "PERMUTIVE_MCP_SERVER_NAME",
    "PERMUTIVE_MCP_TOKEN_ENV",
    "PERMUTIVE_MCP_URL_ENV",
    "Page",
    "PermutiveAPIError",
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
    "SegmentationPayload",
    "SegmentList",
    "Segmentation",
    "ServerError",
    "Source",
    "TransportError",
    "ValidationError",
    "Workspace",
    "WorkspaceList",
    "execute_batch",
]
