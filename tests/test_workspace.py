import json
from PermutiveAPI.Workspace import Workspace, WorkspaceList
from PermutiveAPI.Cohort import Cohort, CohortList
from PermutiveAPI.Audience.Import import Import, ImportList


def test_workspace_serialization_and_properties():
    ws = Workspace(
        name="Main", organisation_id="org1", workspace_id="org1", api_key="k"
    )
    assert ws.is_top_level is True
    assert Workspace.from_json(ws.to_json()) == ws


def test_workspace_list_caches_and_master(tmp_path):
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


def test_workspace_refresh(monkeypatch):
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

    ws.refresh_cohorts()
    ws.refresh_imports()

    assert ws.cohorts()[0].id == "2"
    assert ws.imports()[0].id == "i2"

    monkeypatch.setattr(
        Cohort,
        "list",
        lambda include_child_workspaces=False, api_key="": CohortList.from_json(
            [
                {
                    "name": "C3",
                    "id": "3",
                    "code": "c3",
                    "tags": [],
                    "segment_type": "s3",
                    "workspace_id": "org1",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        Import,
        "list",
        lambda api_key="": ImportList.from_json(
            [
                {
                    "id": "i3",
                    "name": "Import3",
                    "code": "I3",
                    "relation": "rel",
                    "identifiers": ["a"],
                    "source": {"id": "s3", "state": {}, "type": "C"},
                }
            ]
        ),
    )

    assert ws.cohorts(force_refresh=True)[0].id == "3"
    assert ws.imports(force_refresh=True)[0].id == "i3"
