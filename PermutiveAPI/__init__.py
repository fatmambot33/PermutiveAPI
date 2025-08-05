
"""Convenience imports for interacting with the Permutive API.

The :mod:`PermutiveAPI` package exposes classes for managing users, imports,
cohorts and workspaces through Permutive's REST API.  Refer to ``README.md`` in
the repository root for installation and full usage documentation.
"""

from PermutiveAPI.source import Import, ImportList, Segment, SegmentList
from PermutiveAPI.user import Identity, Alias
from PermutiveAPI.cohort import Cohort, CohortList
from PermutiveAPI.workspace import Workspace, WorkspaceList
from PermutiveAPI.utils import customJSONEncoder



__all__ = ["Cohort", "CohortList", "Import", "ImportList", "Segment", "SegmentList",
           "Workspace", "WorkspaceList", "Identity", "Alias", "customJSONEncoder"]
