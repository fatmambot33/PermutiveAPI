import json
import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone
from PermutiveAPI.Audience.Source import Source
from PermutiveAPI.Audience.Segment import Segment, SegmentList
from PermutiveAPI.Audience import _API_ENDPOINT


def test_source_serialization():
    """Test that Source objects can be serialized and deserialized."""
    source = Source(id="s1", state={"active": True}, type="typeA")
    assert Source.from_json(source.to_json()) == source


def test_segment_serialization():
    """Test that Segment objects can be serialized and deserialized."""
    segment = Segment(code="c1", name="Segment1", import_id="imp1", id="1")
    json_data = segment.to_json()
    assert json_data["code"] == "c1"
    recreated = Segment.from_json(
        {
            "code": "c1",
            "name": "Segment1",
            "import_id": "imp1",
            "id": "1",
            "updated_at": segment.updated_at,
        }
    )
    assert recreated == segment


def test_segment_list_caches(tmp_path):
    """Test that SegmentList caches are populated correctly."""
    data = [
        {"code": "c1", "name": "Segment1", "import_id": "imp", "id": "1"},
        {"code": "c2", "name": "Segment2", "import_id": "imp", "id": "2"},
    ]
    json_str = json.dumps(data)
    path = tmp_path / "segments.json"
    path.write_text(json_str)

    for source in (data, json_str, path):
        segments = SegmentList.from_json(source)
        assert segments.id_dictionary["1"].name == "Segment1"
        assert segments.name_dictionary["Segment2"].id == "2"
        assert segments.code_dictionary["c1"].id == "1"


def test_segment_post_init():
    """Test the __post_init__ logic for timestamp normalization."""
    # Test case 1: both created_at and updated_at are None
    segment1 = Segment(code="c1", name="n1", import_id="i1")
    assert segment1.created_at is not None
    assert segment1.updated_at == segment1.created_at

    # Test case 2: created_at is None, updated_at is not
    now = datetime.now(tz=timezone.utc)
    segment2 = Segment(code="c2", name="n2", import_id="i2", updated_at=now)
    assert segment2.created_at == now

    # Test case 3: updated_at is None, created_at is not
    segment3 = Segment(code="c3", name="n3", import_id="i3", created_at=now)
    assert segment3.updated_at == now


@patch.object(Segment, "_request_helper")
def test_segment_create(mock_request_helper):
    """Test successful creation of a segment."""
    segment = Segment(code="c1", name="Segment1", import_id="imp1")
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "new-id",
        "code": "c1",
        "name": "Segment1",
        "import_id": "imp1",
    }
    mock_request_helper.post.return_value = mock_response

    segment.create(api_key="test-key")

    assert segment.id == "new-id"
    mock_request_helper.post.assert_called_once()


@patch.object(Segment, "_request_helper")
def test_segment_create_failure(mock_request_helper):
    """Test that creating a segment raises an error on failure."""
    segment = Segment(code="c1", name="Segment1", import_id="imp1")
    mock_request_helper.post.return_value = None

    with pytest.raises(ValueError, match="Unable to create_segment"):
        segment.create(api_key="test-key")


@patch.object(Segment, "_request_helper")
def test_segment_update(mock_request_helper):
    """Test successful update of a segment."""
    segment = Segment(code="c1", name="Segment1", import_id="imp1", id="seg-id")
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "seg-id",
        "code": "c1",
        "name": "Updated Name",
        "import_id": "imp1",
    }
    mock_request_helper.patch.return_value = mock_response

    segment.update(api_key="test-key")

    assert segment.name == "Updated Name"
    mock_request_helper.patch.assert_called_once()


@patch.object(Segment, "_request_helper")
def test_segment_update_failure(mock_request_helper):
    """Test that updating a segment raises an error on failure."""
    segment = Segment(code="c1", name="Segment1", import_id="imp1", id="seg-id")
    mock_request_helper.patch.return_value = None

    with pytest.raises(ValueError, match="Unable to update_segment"):
        segment.update(api_key="test-key")


@patch.object(Segment, "_request_helper")
def test_segment_delete(mock_request_helper):
    """Test successful deletion of a segment."""
    segment = Segment(code="c1", name="Segment1", import_id="imp1", id="seg-id")
    mock_response = Mock()
    mock_response.status_code = 204
    mock_request_helper.delete.return_value = mock_response

    result = segment.delete(api_key="test-key")

    assert result is None
    mock_request_helper.delete.assert_called_once()


@patch.object(Segment, "_request_helper")
def test_segment_delete_failure(mock_request_helper):
    """Test that deleting a segment raises an error on failure."""
    segment = Segment(code="c1", name="Segment1", import_id="imp1", id="seg-id")
    mock_request_helper.delete.return_value = None

    with pytest.raises(ValueError, match="Response is None"):
        segment.delete(api_key="test-key")


@patch.object(Segment, "_request_helper")
def test_segment_get_by_id(mock_request_helper):
    """Test retrieving a segment by its ID."""
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "seg-id",
        "code": "c1",
        "name": "Segment1",
        "import_id": "imp1",
    }
    mock_request_helper.get.return_value = mock_response

    segment = Segment.get_by_id(
        import_id="imp1", segment_id="seg-id", api_key="test-key"
    )

    assert segment.name == "Segment1"
    mock_request_helper.get.assert_called_once()


@patch.object(Segment, "_request_helper")
def test_segment_get_by_id_failure(mock_request_helper):
    """Test that getting a segment by ID raises an error on failure."""
    mock_request_helper.get.return_value = None

    with pytest.raises(ValueError, match="Unable to get_by_id"):
        Segment.get_by_id(import_id="imp1", segment_id="seg-id", api_key="test-key")


@patch.object(Segment, "_request_helper")
def test_segment_get_by_code(mock_request_helper):
    """Test retrieving a segment by its code."""
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "seg-id",
        "code": "c1",
        "name": "Segment1",
        "import_id": "imp1",
    }
    mock_request_helper.get.return_value = mock_response

    segment = Segment.get_by_code(
        import_id="imp1", segment_code="c1", api_key="test-key"
    )

    assert segment.name == "Segment1"
    mock_request_helper.get.assert_called_once()


@patch.object(Segment, "_request_helper")
def test_segment_get_by_code_failure(mock_request_helper):
    """Test that getting a segment by code raises an error on failure."""
    mock_request_helper.get.return_value = None

    with pytest.raises(ValueError, match="Unable to get_segment"):
        Segment.get_by_code(
            import_id="imp1", segment_code="seg-code", api_key="test-key"
        )


@patch.object(Segment, "_request_helper")
def test_segment_list_pagination(mock_request_helper):
    """Test that the list method correctly handles pagination."""
    # Mock first page response
    mock_response_page1 = Mock()
    mock_response_page1.json.return_value = {
        "elements": [{"id": "1", "name": "s1", "code": "c1", "import_id": "imp1"}],
        "pagination": {"next_token": "token2"},
    }
    # Mock second page response
    mock_response_page2 = Mock()
    mock_response_page2.json.return_value = {
        "elements": [{"id": "2", "name": "s2", "code": "c2", "import_id": "imp1"}],
        "pagination": {},
    }
    mock_request_helper.get.side_effect = [
        mock_response_page1,
        mock_response_page2,
    ]

    segments = Segment.list(import_id="imp1", api_key="test-key")

    assert len(segments) == 2
    assert segments[0].id == "1"
    assert segments[1].id == "2"
    assert mock_request_helper.get.call_count == 2
    mock_request_helper.get.assert_any_call(
        "test-key",
        f"{_API_ENDPOINT}/imp1/segments",
        params={},
    )
    mock_request_helper.get.assert_any_call(
        "test-key",
        f"{_API_ENDPOINT}/imp1/segments",
        params={"pagination_token": "token2"},
    )


def test_segment_list_cache_refresh():
    """Test that SegmentList caches are refreshed when accessed."""
    segments = SegmentList([])
    # Caches start empty
    assert not segments._id_dictionary_cache
    assert not segments._name_dictionary_cache
    assert not segments._code_dictionary_cache

    # Populate the list
    segments.append(Segment(code="c1", name="Segment1", import_id="imp", id="1"))

    # Accessing a property should trigger rebuild
    assert segments.id_dictionary["1"].name == "Segment1"
    assert segments._id_dictionary_cache  # Cache should now be populated

    # Reset and test another property
    segments._name_dictionary_cache = {}
    assert not segments._name_dictionary_cache
    assert segments.name_dictionary["Segment1"].id == "1"
    assert segments._name_dictionary_cache

    # And the last one
    segments._code_dictionary_cache = {}
    assert not segments._code_dictionary_cache
    assert segments.code_dictionary["c1"].id == "1"
    assert segments._code_dictionary_cache
