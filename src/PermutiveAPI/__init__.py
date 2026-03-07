"""Convenience imports for interacting with the Permutive API.

The `PermutiveAPI` package exposes classes for managing users, imports,
cohorts and workspaces through Permutive's REST API.  Refer to ``README.md`` in
the repository root for installation and full usage documentation.
"""

from .audience import Import, ImportList, Segment, SegmentList, Source
from .identify import Identity, Alias
from .cohort import Cohort, CohortList
from .workspace import Workspace, WorkspaceList
from .segmentation import Event, Segmentation
from .context import ContextSegment
from .utils.http import (
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
)

__all__ = [
    "Cohort",
    "CohortList",
    "Import",
    "ImportList",
    "Segment",
    "SegmentList",
    "Source",
    "Workspace",
    "WorkspaceList",
    "Identity",
    "Alias",
    "Event",
    "Segmentation",
    "ContextSegment",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveRateLimitError",
    "PermutiveResourceNotFoundError",
    "PermutiveServerError",
]
