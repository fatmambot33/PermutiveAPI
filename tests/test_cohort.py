import json
import pytest
from unittest.mock import Mock, patch
from PermutiveAPI.Cohort import Cohort, CohortList


def test_cohort_serialization():
    """Test that Cohort objects can be serialized and deserialized."""
    cohort = Cohort(
        name="C1",
        id="1",
        code="code1",
        tags=["t1"],
        workspace_id="w1",
        segment_type="type1",
    )
    json_data = cohort.to_json()
    assert json_data["name"] == "C1"
    recreated = Cohort.from_json(
        {
            "name": "C1",
            "id": "1",
            "code": "code1",
            "tags": ["t1"],
            "workspace_id": "w1",
            "segment_type": "type1",
            "created_at": cohort.created_at,
            "last_updated_at": cohort.last_updated_at,
        }
    )
    assert recreated == cohort


def test_cohort_list_caches(tmp_path):
    """Test that CohortList caches are populated correctly."""
    data = [
        {
            "name": "C1",
            "id": "1",
            "code": "c1",
            "tags": ["t1"],
            "segment_type": "s1",
            "workspace_id": "w1",
        },
        {
            "name": "C2",
            "id": "2",
            "code": "c2",
            "tags": ["t1", "t2"],
            "segment_type": "s2",
            "workspace_id": "w2",
        },
    ]
    json_str = json.dumps(data)
    path = tmp_path / "cohorts.json"
    path.write_text(json_str)

    for source in (data, json_str, path):
        cohorts = CohortList.from_json(source)
        assert cohorts.id_dictionary["1"].name == "C1"
        assert cohorts.code_dictionary["c2"].id == "2"
        assert cohorts.name_dictionary["C2"].code == "c2"
        assert cohorts.tag_dictionary["t2"][0].id == "2"
        assert cohorts.segment_type_dictionary["s1"][0].id == "1"
        assert cohorts.workspace_dictionary["w1"][0].name == "C1"


@patch.object(Cohort, "_request_helper")
def test_cohort_create(mock_request_helper):
    """Test successful creation of a cohort."""
    cohort = Cohort(name="New Cohort", query={"type": "test"})
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "new-id",
        "code": "new-code",
        "name": "New Cohort",
    }
    mock_request_helper.post_static.return_value = mock_response

    cohort.create(api_key="test-key")

    assert cohort.id == "new-id"
    assert cohort.code == "new-code"
    mock_request_helper.post_static.assert_called_once()


@patch.object(Cohort, "_request_helper")
def test_cohort_create_with_id_warning(mock_request_helper, caplog):
    """Test that a warning is logged if a cohort is created with an existing ID."""
    cohort = Cohort(name="New Cohort", query={"type": "test"}, id="existing-id")
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "new-id",
        "code": "new-code",
        "name": "New Cohort",
    }
    mock_request_helper.post_static.return_value = mock_response

    cohort.create(api_key="test-key")
    assert "id is specified" in caplog.text


@patch.object(Cohort, "_request_helper")
def test_cohort_update(mock_request_helper):
    """Test successful update of a cohort."""
    cohort = Cohort(name="C1", id="1")
    mock_response = Mock()
    mock_response.json.return_value = {"id": "1", "name": "Updated Name"}
    mock_request_helper.patch_static.return_value = mock_response

    updated_cohort = cohort.update(api_key="test-key")

    assert updated_cohort.name == "Updated Name"
    mock_request_helper.patch_static.assert_called_once()


@patch.object(Cohort, "_request_helper")
def test_cohort_delete(mock_request_helper):
    """Test successful deletion of a cohort."""
    cohort = Cohort(name="C1", id="1")
    mock_request_helper.delete_static.return_value = Mock(status_code=204)

    cohort.delete(api_key="test-key")

    mock_request_helper.delete_static.assert_called_once()


@patch.object(Cohort, "_request_helper")
def test_cohort_get_by_id(mock_request_helper):
    """Test retrieving a cohort by its ID."""
    mock_response = Mock()
    mock_response.json.return_value = {"id": "1", "name": "Test Cohort"}
    mock_request_helper.get_static.return_value = mock_response

    cohort = Cohort.get_by_id(id="1", api_key="test-key")

    assert cohort.name == "Test Cohort"
    mock_request_helper.get_static.assert_called_once()


@patch.object(Cohort, "list")
def test_get_by_name(mock_list):
    """Test retrieving a cohort by its name."""
    cohorts_data = [
        {"name": "C1", "id": "1"},
        {"name": "C2", "id": "2"},
    ]
    mock_list.return_value = CohortList.from_json(cohorts_data)

    result = Cohort.get_by_name("C1", api_key="test-key")
    assert result is not None
    assert result.id == "1"


@patch.object(Cohort, "list")
def test_get_by_code(mock_list):
    """Test retrieving a cohort by its code."""
    cohorts_data = [
        {"name": "C1", "id": "1", "code": "101"},
        {"name": "C2", "id": "2", "code": "102"},
    ]
    mock_list.return_value = CohortList.from_json(cohorts_data)

    result = Cohort.get_by_code("101", api_key="test-key")
    assert result is not None
    assert result.id == "1"


@patch.object(Cohort, "_request_helper")
def test_cohort_list_with_children(mock_request_helper):
    """Test that the list method includes child workspaces when requested."""
    mock_response = Mock()
    mock_response.json.return_value = [{"id": "1", "name": "Child Cohort"}]
    mock_request_helper.get_static.return_value = mock_response

    Cohort.list(api_key="test-key", include_child_workspaces=True)

    mock_request_helper.get_static.assert_called_with(
        "test-key",
        "https://api.permutive.app/cohorts-api/v2/cohorts/?include-child-workspaces=true",
    )


def test_cohort_list_cache_rebuild():
    """Test that CohortList caches are rebuilt when accessed."""
    cohorts = CohortList([])
    assert not cohorts._id_dictionary_cache

    cohorts.append(Cohort(name="C1", id="1"))
    assert cohorts.id_dictionary["1"].name == "C1"
    assert cohorts._id_dictionary_cache

    cohorts._code_dictionary_cache = {}
    assert not cohorts._code_dictionary_cache
    cohorts[0].code = "c1"
    assert cohorts.code_dictionary["c1"].id == "1"
    assert cohorts._code_dictionary_cache

    cohorts._name_dictionary_cache = {}
    assert not cohorts._name_dictionary_cache
    assert cohorts.name_dictionary["C1"].id == "1"
    assert cohorts._name_dictionary_cache

    cohorts._tag_dictionary_cache = {}
    assert not cohorts._tag_dictionary_cache
    cohorts[0].tags = ["t1"]
    assert cohorts.tag_dictionary["t1"][0].id == "1"
    assert cohorts._tag_dictionary_cache

    cohorts._segment_type_dictionary_cache = {}
    assert not cohorts._segment_type_dictionary_cache
    cohorts[0].segment_type = "s1"
    assert cohorts.segment_type_dictionary["s1"][0].id == "1"
    assert cohorts._segment_type_dictionary_cache

    cohorts._workspace_dictionary_cache = {}
    assert not cohorts._workspace_dictionary_cache
    cohorts[0].workspace_id = "w1"
    assert cohorts.workspace_dictionary["w1"][0].id == "1"
    assert cohorts._workspace_dictionary_cache


def test_cohort_activate_stub():
    """Ensure activate stub raises NotImplementedError."""
    cohort = Cohort(name="C1")
    with pytest.raises(NotImplementedError):
        cohort.activate(api_key="test-key")


def test_cohort_archive_stub():
    """Ensure archive stub raises NotImplementedError."""
    cohort = Cohort(name="C1")
    with pytest.raises(NotImplementedError):
        cohort.archive(api_key="test-key")
