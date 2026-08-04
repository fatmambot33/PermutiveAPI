"""Public imports for the typed Permutive API SDK."""

from .audience import Import, ImportList, Segment, SegmentList, Source
from .cohort import Cohort, CohortList
from .context import ContextSegment
from .identify import Alias, Identity
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
from .segmentation import Event, Segmentation
from .utils.http import (
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
)
from .workspace import Workspace, WorkspaceList

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
