import json
from unittest.mock import Mock, patch

import pytest

from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.http import PermutiveAPIError
from PermutiveAPI.Cohort import Cohort, CohortList, _API_ENDPOINT


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


def test_cohort_list_to_pd_dataframe():
    """Ensure ``to_pd_dataframe`` converts cohorts into a pandas ``DataFrame``."""

    cohorts = CohortList(
        [
            Cohort(name="C1", id="1", code="c1", tags=["t1"]),
            Cohort(name="C2", id="2", description="second cohort"),
        ]
    )

    df = cohorts.to_pd_dataframe()

    assert df.shape[0] == 2
    assert set(df["name"]) == {"C1", "C2"}
    assert "code" in df.columns


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
    mock_request_helper.post.return_value = mock_response

    cohort.create(api_key="test-key")

    assert cohort.id == "new-id"
    assert cohort.code == "new-code"
    mock_request_helper.post.assert_called_once()


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
    mock_request_helper.post.return_value = mock_response

    cohort.create(api_key="test-key")
    assert "id is specified" in caplog.text


@patch.object(Cohort, "_request_helper")
def test_cohort_update(mock_request_helper):
    """Test successful update of a cohort."""
    cohort = Cohort(name="C1", id="1")
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "1",
        "name": "Updated Name",
        "last_updated_at": "2022-01-01T00:00:00Z",
    }
    mock_request_helper.patch.return_value = mock_response

    cohort.update(api_key="test-key")
    assert cohort.last_updated_at is not None
    mock_request_helper.patch.assert_called_once()


@patch.object(Cohort, "_request_helper")
def test_cohort_update_failure(mock_request_helper):
    """Test that updating a cohort raises an error on failure."""
    cohort = Cohort(name="C1", id="1")
    mock_request_helper.patch.return_value = None

    with pytest.raises(ValueError, match="Response is None"):
        cohort.update(api_key="test-key")


@patch.object(Cohort, "_request_helper")
def test_cohort_delete(mock_request_helper):
    """Test successful deletion of a cohort."""
    cohort = Cohort(name="C1", id="1")
    mock_request_helper.delete.return_value = Mock(status_code=204)

    cohort.delete(api_key="test-key")

    mock_request_helper.delete.assert_called_once()


@patch.object(Cohort, "_request_helper")
def test_cohort_delete_failure(mock_request_helper):
    """Test that deleting a cohort without an ID raises an error."""
    cohort = Cohort(name="C1", id=None)
    with pytest.raises(ValueError, match="Cohort ID must be specified for deletion."):
        cohort.delete(api_key="test-key")


@patch.object(Cohort, "_request_helper")
def test_cohort_get_by_id(mock_request_helper):
    """Test retrieving a cohort by its ID."""
    mock_response = Mock()
    mock_response.json.return_value = {"id": "1", "name": "Test Cohort"}
    mock_request_helper.get.return_value = mock_response

    cohort = Cohort.get_by_id(id="1", api_key="test-key")

    assert cohort.name == "Test Cohort"
    mock_request_helper.get.assert_called_once()


@patch.object(Cohort, "get_by_id")
@patch.object(Cohort, "list")
def test_get_by_name(mock_list, mock_get_by_id):
    """Test retrieving a cohort by its name."""
    cohorts_data = [
        {"name": "C1", "id": "1"},
        {"name": "C2", "id": "2"},
    ]
    mock_list.return_value = CohortList.from_json(cohorts_data)
    mock_get_by_id.return_value = Cohort.from_json(cohorts_data[0])

    result = Cohort.get_by_name("C1", api_key="test-key")
    assert result is not None
    assert result.id == "1"
    mock_get_by_id.assert_called_once_with("1", "test-key")


@patch.object(Cohort, "get_by_id")
@patch.object(Cohort, "list")
def test_get_by_code(mock_list, mock_get_by_id):
    """Test retrieving a cohort by its code."""
    cohorts_data = [
        {"name": "C1", "id": "1", "code": "101"},
        {"name": "C2", "id": "2", "code": "102"},
    ]
    mock_list.return_value = CohortList.from_json(cohorts_data)
    mock_get_by_id.return_value = Cohort.from_json(cohorts_data[0])

    result = Cohort.get_by_code("101", api_key="test-key")
    assert result is not None
    assert result.id == "1"
    mock_get_by_id.assert_called_once_with("1", "test-key")


@patch.object(Cohort, "_request_helper")
def test_cohort_list_with_children(mock_request_helper):
    """Test that the list method includes child workspaces when requested."""
    mock_response = Mock()
    mock_response.json.return_value = [{"id": "1", "name": "Child Cohort"}]
    mock_request_helper.get.return_value = mock_response

    Cohort.list(api_key="test-key", include_child_workspaces=True)

    mock_request_helper.get.assert_called_with(
        "test-key",
        _API_ENDPOINT,
        params={"include-child-workspaces": "true"},
    )


def test_cohort_list_cache_refresh():
    """Test that CohortList caches are refreshed when accessed."""
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


def test_cohort_batch_create_updates_instances(fake_thread_pool, monkeypatch):
    """Batch creation should mutate cohort instances with API payloads."""

    payloads = {
        "Alpha": {
            "id": "id-alpha",
            "code": "code-alpha",
            "name": "Alpha",
            "request_id": "req-alpha",
        },
        "Beta": {
            "id": "id-beta",
            "code": "code-beta",
            "name": "Beta",
            "request_id": "req-beta",
        },
    }

    def fake_request(method, api_key, url, **kwargs):  # noqa: ANN001 - mirror signature
        name = kwargs["json"]["name"]
        response = Mock()
        response.json.return_value = payloads[name]
        return response

    monkeypatch.setattr(http, "request", fake_request)

    cohorts = [
        Cohort(name="Alpha", query={"type": "example"}),
        Cohort(name="Beta", query={"type": "example"}),
    ]

    progress = []
    responses, errors = Cohort.batch_create(
        cohorts,
        api_key="test-key",
        max_workers=2,
        progress_callback=progress.append,
    )

    assert len(fake_thread_pool) == 1
    assert fake_thread_pool[0].max_workers == 2
    assert len(responses) == 2
    assert errors == []

    assert {cohort.id for cohort in cohorts} == {"id-alpha", "id-beta"}
    assert {cohort.code for cohort in cohorts} == {"code-alpha", "code-beta"}
    assert {cohort.request_id for cohort in cohorts} == {"req-alpha", "req-beta"}

    assert len(progress) == 2
    assert [p.completed for p in progress] == [1, 2]
    assert progress[-1].errors == 0


def test_cohort_batch_create_propagates_errors(fake_thread_pool, monkeypatch):
    """Ensure batch creation surfaces API errors and partial success."""

    def fake_request(method, api_key, url, **kwargs):  # noqa: ANN001 - mirror signature
        name = kwargs["json"]["name"]
        if name == "Failing":
            raise PermutiveAPIError("cohort failed")
        response = Mock()
        response.json.return_value = {
            "id": f"id-{name.lower()}",
            "code": f"code-{name.lower()}",
            "name": name,
        }
        return response

    monkeypatch.setattr(http, "request", fake_request)

    cohorts = [
        Cohort(name="Working", query={"type": "example"}),
        Cohort(name="Failing", query={"type": "example"}),
    ]

    progress = []
    responses, errors = Cohort.batch_create(
        cohorts,
        api_key="test-key",
        max_workers=1,
        progress_callback=progress.append,
    )

    assert len(fake_thread_pool) == 1
    assert fake_thread_pool[0].max_workers == 1
    assert len(responses) == 1
    assert len(errors) == 1

    failing_request, exception = errors[0]
    assert failing_request.json is not None
    assert failing_request.json["name"] == "Failing"
    assert isinstance(exception, PermutiveAPIError)

    assert cohorts[0].id == "id-working"
    assert cohorts[1].id is None

    assert len(progress) == 2
    error_counts = [p.errors for p in progress]
    assert error_counts[-1] == len(errors)
    assert all(0 <= count <= len(errors) for count in error_counts)
