from PermutiveAPI.Workspace import Workspace, WorkspaceList


def test_workspace_serialization_and_properties():
    ws = Workspace(
        name="Main", organisation_id="org1", workspace_id="org1", api_key="k"
    )
    assert ws.is_top_level is True
    assert Workspace.from_json(ws.to_json()) == ws


def test_workspace_list_caches_and_master():
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
