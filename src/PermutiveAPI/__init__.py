"""Convenience imports for interacting with the Permutive API.

The package root exposes the stable public SDK contract. Internal helpers must
be imported from their implementation modules and are not compatibility-bound.
"""

from .audience import Import, ImportList, Segment, SegmentList, Source
from .batch import BatchItemResult, BatchResult
from .client import ClientConfig, PermutiveClient, Transport
from .cohort import Cohort, CohortList
from .context import ContextSegment
from .identify import Alias, Identity
from .pagination import Page, iter_pages
from .segmentation import Event, Segmentation
from .types import JSONArray, JSONObject, JSONScalar, JSONValue
from .workspace import Workspace, WorkspaceList
from .utils.http import (
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
)

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
