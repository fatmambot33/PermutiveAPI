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


def test_collection_lookup_caches_refresh_after_mutations() -> None:
    """Mutable legacy collections keep local lookup indexes current."""
    cohort_a = Cohort(name="Sports", id="cohort-1", code="101")
    cohort_b = Cohort(name="News", id="cohort-2", code="202")
    cohorts = CohortList([cohort_a])
    cohorts.append(cohort_b)
    assert cohorts.by_id("cohort-2") is cohort_b
    cohorts[0] = cohort_b
    assert cohorts.by_id("cohort-1") is None
    cohorts.remove(cohort_b)
    assert cohorts.by_name("News") is None

    source = Source(id="source-1", state={}, type="test")
    import_a = Import(
        id="import-1",
        name="Customers",
        code="customers",
        relation="customer",
        identifiers=["email"],
        source=source,
    )
    import_b = Import(
        id="import-2",
        name="Prospects",
        code="prospects",
        relation="customer",
        identifiers=["email"],
        source=source,
    )
    imports = ImportList([import_a])
    imports.append(import_b)
    assert imports.by_code("prospects") is import_b
    imports.remove(import_b)
    assert imports.by_id("import-2") is None

    segment_a = Segment(
        code="vip", name="VIP", import_id="import-1", id="segment-1"
    )
    segment_b = Segment(
        code="loyal", name="Loyal", import_id="import-1", id="segment-2"
    )
    segments = SegmentList([segment_a])
    segments.append(segment_b)
    assert segments.by_name("Loyal") is segment_b
    segments.remove(segment_b)
    assert segments.by_code("loyal") is None

    workspace_a = Workspace(
        name="Main",
        organisation_id="workspace-1",
        workspace_id="workspace-1",
        api_key="test-key",
    )
    workspace_b = Workspace(
        name="Child",
        organisation_id="workspace-2",
        workspace_id="workspace-2",
        api_key="test-key",
    )
    workspaces = WorkspaceList([workspace_a])
    workspaces.append(workspace_b)
    assert workspaces.by_id("workspace-2") is workspace_b
    workspaces.remove(workspace_b)
    assert workspaces.by_name("Child") is None
