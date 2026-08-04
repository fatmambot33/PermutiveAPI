"""Public imports for the typed Permutive API SDK."""

from __future__ import annotations

from importlib import import_module
from typing import Any

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
    PermutiveClient,
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
    "AuthenticationError",
    "AuthorizationError",
    "BatchItem",
    "BatchResult",
    "Cohort",
    "CohortList",
    "ConflictError",
    "ContextSegment",
    "DecodingError",
    "Event",
    "Identity",
    "Import",
    "ImportList",
    "JSONObject",
    "JSONScalar",
    "JSONValue",
    "NotFoundError",
    "Page",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveClient",
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
    "ServerError",
    "Source",
    "TransportError",
    "ValidationError",
    "Workspace",
    "WorkspaceList",
    "execute_batch",
]
