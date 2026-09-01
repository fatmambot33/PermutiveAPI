"""Tests for explicit in-memory collection lookup helpers."""

from PermutiveAPI import (
    Cohort,
    CohortList,
    Import,
    ImportList,
    Segment,
    SegmentList,
    Workspace,
    WorkspaceList,
)
from PermutiveAPI.audience.source import Source


def test_cohort_list_lookup_helpers() -> None:
    """CohortList resolves cached identity fields without remote access."""
    cohort = Cohort(name="Sports", id="cohort-1", code="101")
    cohorts = CohortList([cohort])

    assert cohorts.by_id("cohort-1") is cohort
    assert cohorts.by_name("Sports") is cohort
    assert cohorts.by_code("101") is cohort
    assert cohorts.by_code(101) is cohort
    assert cohorts.by_id("missing") is None
    assert cohorts.by_name("missing") is None
    assert cohorts.by_code("missing") is None


def test_import_list_lookup_helpers() -> None:
    """ImportList resolves cached identity fields without remote access."""
    source = Source(id="source-1", state={}, type="test")
    import_ = Import(
        id="import-1",
        name="Customers",
        code="customers",
        relation="customer",
        identifiers=["email"],
        source=source,
    )
    imports = ImportList([import_])

    assert imports.by_id("import-1") is import_
    assert imports.by_name("Customers") is import_
    assert imports.by_code("customers") is import_
    assert imports.by_id("missing") is None
    assert imports.by_name("missing") is None
    assert imports.by_code("missing") is None


def test_segment_list_lookup_helpers() -> None:
    """SegmentList resolves cached identity fields without remote access."""
    segment = Segment(
        code="vip",
        name="VIP",
        import_id="import-1",
        id="segment-1",
    )
    segments = SegmentList([segment])

    assert segments.by_id("segment-1") is segment
    assert segments.by_name("VIP") is segment
    assert segments.by_code("vip") is segment
    assert segments.by_id("missing") is None
    assert segments.by_name("missing") is None
    assert segments.by_code("missing") is None


def test_workspace_list_lookup_helpers() -> None:
    """WorkspaceList resolves cached identity fields without remote access."""
    workspace = Workspace(
        name="Main",
        organisation_id="workspace-1",
        workspace_id="workspace-1",
        api_key="test-key",
    )
    workspaces = WorkspaceList([workspace])

    assert workspaces.by_id("workspace-1") is workspace
    assert workspaces.by_name("Main") is workspace
    assert workspaces.by_id("missing") is None
    assert workspaces.by_name("missing") is None
