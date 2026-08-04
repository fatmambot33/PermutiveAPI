"""Convenience imports for interacting with the Permutive API.

The typed core imports without optional dataframe dependencies. Legacy resource
classes are loaded lazily when accessed, preserving the package-root API.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Dict, Tuple

from .batch import BatchItemResult, BatchResult
from .client import ClientConfig, PermutiveClient, Transport
from .pagination import Page, iter_pages
from .types import JSONArray, JSONObject, JSONScalar, JSONValue
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

_LAZY_EXPORTS: Dict[str, Tuple[str, str]] = {
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


def __getattr__(name: str) -> object:
    """Load a legacy public resource only when it is first accessed."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute_name = target
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


__all__ = [
    "Alias",
    "BatchItemResult",
    "BatchResult",
    "ClientConfig",
    "Cohort",
    "CohortList",
    "ContextSegment",
    "Event",
    "Identity",
    "Import",
    "ImportList",
    "JSONArray",
    "JSONObject",
    "JSONScalar",
    "JSONValue",
    "Page",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveClient",
    "PermutiveRateLimitError",
    "PermutiveResourceNotFoundError",
    "PermutiveServerError",
    "Segment",
    "SegmentList",
    "Segmentation",
    "Source",
    "Transport",
    "Workspace",
    "WorkspaceList",
    "iter_pages",
]
