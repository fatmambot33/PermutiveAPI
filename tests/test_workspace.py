import json
import pytest
from PermutiveAPI.Workspace import Workspace, WorkspaceList
from PermutiveAPI.Cohort import Cohort, CohortList
from PermutiveAPI.Audience.Import import Import, ImportList
from PermutiveAPI.Audience.Segment import Segment, SegmentList


def test_workspace_serialization_and_properties():
    """Test Workspace serialization and property access."""
    ws = Workspace(
        name="Main", organisation_id="org1", workspace_id="org1", api_key="k"
    )
    assert ws.is_top_level is True
    assert Workspace.from_json(ws.to_json()) == ws


def test_workspace_list_caches_and_master(tmp_path):
    """Test WorkspaceList caching and master workspace detection."""
    data = [
        {
            "name": "Main",
            "organisation_id": "org1",
            "workspace_id": "org1",
            "api_key": "k",
        },
        {
            "name": "Child",
            "organisation_id": "org1",
            "workspace_id": "child",
            "api_key": "k2",
        },
    ]

    workspaces = WorkspaceList.from_json(data)
    assert workspaces.master_workspace.name == "Main"
    assert workspaces.id_dictionary["child"].name == "Child"
    assert workspaces.name_dictionary["Main"].workspace_id == "org1"


def test_workspace_list_no_master():
    """Test that ValueError is raised when no master workspace is found."""
    data = [
        {
            "name": "Child",
            "organisation_id": "org1",
            "workspace_id": "child",
            "api_key": "k2",
        },
    ]
    workspaces = WorkspaceList.from_json(data)
    with pytest.raises(ValueError, match="No top-level workspace found"):
        _ = workspaces.master_workspace


def test_workspace_segments_cache(monkeypatch):
    """Test the caching logic for the segments method."""
    ws = Workspace(
        name="Main", organisation_id="org1", workspace_id="org1", api_key="k"
    )
    segment_data1 = [{"id": "s1", "code": "c1", "name": "Segment1", "import_id": "i1"}]
    segment_data2 = [{"id": "s2", "code": "c2", "name": "Segment2", "import_id": "i1"}]

    # Initial call, should cache segment_data1
    monkeypatch.setattr(
        Segment, "list", lambda import_id, api_key: SegmentList.from_json(segment_data1)
    )
    assert ws.segments(import_id="i1")[0].id == "s1"

    # Subsequent call without force_refresh, should return cached data
    monkeypatch.setattr(
        Segment, "list", lambda import_id, api_key: SegmentList.from_json(segment_data2)
    )
    assert ws.segments(import_id="i1")[0].id == "s1"

    # Call with force_refresh, should return new data
    assert ws.segments(import_id="i1", force_refresh=True)[0].id == "s2"


def test_workspace_refresh(monkeypatch):
    """Test the refresh logic for cohorts and imports within a workspace."""
    ws = Workspace(
        name="Main", organisation_id="org1", workspace_id="org1", api_key="k"
    )

    cohort_data1 = [
        {
            "name": "C1",
            "id": "1",
            "code": "c1",
            "tags": [],
            "segment_type": "s1",
            "workspace_id": "org1",
        }
    ]
    cohort_data2 = [
        {
            "name": "C2",
            "id": "2",
            "code": "c2",
            "tags": [],
            "segment_type": "s2",
            "workspace_id": "org1",
        }
    ]
    import_data1 = [
        {
            "id": "i1",
            "name": "Import1",
            "code": "I1",
            "relation": "rel",
            "identifiers": ["a"],
            "source": {"id": "s1", "state": {}, "type": "A"},
        }
    ]
    import_data2 = [
        {
            "id": "i2",
            "name": "Import2",
            "code": "I2",
            "relation": "rel",
            "identifiers": ["a"],
            "source": {"id": "s2", "state": {}, "type": "B"},
        }
    ]

    # Initial call, should cache cohort_data1 and import_data1
    monkeypatch.setattr(
        Cohort,
        "list",
        lambda include_child_workspaces=False, api_key="": CohortList.from_json(
            cohort_data1
        ),
    )
    monkeypatch.setattr(
        Import,
        "list",
        lambda api_key="": ImportList.from_json(import_data1),
    )
    assert ws.cohorts()[0].id == "1"
    assert ws.imports()[0].id == "i1"

    # Subsequent call without force_refresh, should return cached data
    monkeypatch.setattr(
        Cohort,
        "list",
        lambda include_child_workspaces=False, api_key="": CohortList.from_json(
            cohort_data2
        ),
    )
    monkeypatch.setattr(
        Import,
        "list",
        lambda api_key="": ImportList.from_json(import_data2),
    )
    assert ws.cohorts()[0].id == "1"
    assert ws.imports()[0].id == "i1"

    # Call with force_refresh, should return new data
    assert ws.cohorts(force_refresh=True)[0].id == "2"
    assert ws.imports(force_refresh=True)[0].id == "i2"
