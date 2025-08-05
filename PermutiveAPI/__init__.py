
"""Convenience imports for interacting with the Permutive API.

The :mod:`PermutiveAPI` package exposes classes for managing users, imports,
cohorts and workspaces through Permutive's REST API.  Refer to ``README.md`` in
the repository root for installation and full usage documentation.
"""

from .source import Import, ImportList, Segment, SegmentList
from .user import Identity, Alias
from .cohort import Cohort, CohortList
from .workspace import Workspace, WorkspaceList
from .utils import customJSONEncoder



__all__ = ["Cohort", "CohortList", "Import", "ImportList", "Segment", "SegmentList",
           "Workspace", "WorkspaceList", "Identity", "Alias", "customJSONEncoder"]
