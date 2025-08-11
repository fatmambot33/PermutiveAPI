"""Convenience imports for interacting with the Permutive API.

The `PermutiveAPI` package exposes classes for managing users, imports,
cohorts and workspaces through Permutive's REST API.  Refer to ``README.md`` in
the repository root for installation and full usage documentation.
"""

from PermutiveAPI.Audience.Source import Source
from PermutiveAPI.Audience.Import import Import, ImportList
from PermutiveAPI.Audience.Segment import Segment, SegmentList
from PermutiveAPI.Identify.Identity import Identity
from PermutiveAPI.Identify.Alias import Alias
from PermutiveAPI.Cohort import Cohort, CohortList
from PermutiveAPI.Workspace import Workspace, WorkspaceList
from PermutiveAPI.Utils import customJSONEncoder


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
    "customJSONEncoder",
]
