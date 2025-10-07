import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from PermutiveAPI.Audience.Import import Import, ImportList
from PermutiveAPI.Audience.Source import Source


def test_import_list_from_json_and_caches(tmp_path):
    """Test deserialization and caching of ImportList.

    Verify that from_json correctly handles various input formats (dict,
    JSON string, file path) and that the lookup caches
    (id_dictionary, name_dictionary, identifier_dictionary) are
    populated correctly.
    """
    data = [
        {
            "id": "1",
            "name": "Import1",
            "code": "I1",
            "relation": "rel",
            "identifiers": ["a", "b"],
            "source": {"id": "s1", "state": {}, "type": "A"},
        },
        {
            "id": "2",
            "name": "Import2",
            "code": "I2",
            "relation": "rel",
            "identifiers": ["b"],
            "source": {"id": "s2", "state": {}, "type": "B"},
        },
    ]
    json_str = json.dumps(data)
    path = tmp_path / "imports.json"
    path.write_text(json_str)

    for source in (data, json_str, path):
        imports = ImportList.from_json(source)
        assert imports.id_dictionary["1"].name == "Import1"
        assert imports.name_dictionary["Import2"].id == "2"
        assert imports.identifier_dictionary["b"][0].id == "1"
        assert isinstance(imports[0].source, Source)


def test_import_list_to_pd_dataframe():
    """Ensure ``to_pd_dataframe`` converts imports into a pandas ``DataFrame``."""

    source = Source(id="s1", state={}, type="type")
    imports = ImportList(
        [
            Import(
                id="1",
                name="Import1",
                code="I1",
                relation="rel1",
                identifiers=["a"],
                source=source,
            ),
            Import(
                id="2",
                name="Import2",
                code="I2",
                relation="rel2",
                identifiers=["b", "c"],
                source=source,
            ),
        ]
    )

    df = imports.to_pd_dataframe()

    assert df.shape[0] == 2
    assert set(df["name"]) == {"Import1", "Import2"}
    assert "identifiers" in df.columns


@patch.object(Import, "_request_helper")
def test_import_list_method(mock_request_helper):
    """Test the Import.list method.

    Verify that the `list` method correctly calls the request helper and
    deserializes the response into an ImportList.
    """
    mock_response = Mock()
    mock_response.json.return_value = {
        "items": [
            {
                "id": "1",
                "name": "Import1",
                "code": "c1",
                "relation": "r1",
                "identifiers": ["i1"],
                "source": {"id": "s1", "state": {}, "type": "t1"},
            }
        ]
    }
    mock_request_helper.get.return_value = mock_response

    imports = Import.list(api_key="test-key")

    assert len(imports) == 1
    assert imports[0].name == "Import1"
    mock_request_helper.get.assert_called_once()


@patch.object(Import, "_request_helper")
def test_import_list_failure(mock_request_helper):
    """Test the Import.list method failure case."""
    mock_request_helper.get.return_value = None

    with pytest.raises(ValueError, match="Response is None"):
        Import.list(api_key="test-key")


@patch.object(Import, "_request_helper")
def test_import_get_by_id(mock_request_helper):
    """Test the Import.get_by_id method."""
    mock_response = Mock()
    mock_response.json.return_value = {
        "id": "1",
        "name": "Import1",
        "code": "c1",
        "relation": "r1",
        "identifiers": ["i1"],
        "source": {"id": "s1", "state": {}, "type": "t1"},
    }
    mock_request_helper.get.return_value = mock_response

    imp = Import.get_by_id(id="1", api_key="test-key")

    assert imp.name == "Import1"
    mock_request_helper.get.assert_called_once()


@patch.object(Import, "_request_helper")
def test_import_get_by_id_failure(mock_request_helper):
    """Test the Import.get_by_id method failure case."""
    mock_request_helper.get.return_value = None

    with pytest.raises(ValueError, match="Unable to get_import"):
        Import.get_by_id(id="1", api_key="test-key")


def test_import_post_init():
    """Test the __post_init__ logic for timestamp normalization."""
    # Test case 1: both created_at and updated_at are None
    imp1 = Import(
        id="1",
        name="n1",
        code="c1",
        relation="r1",
        identifiers=[],
        source=Source(id="s1", state={}, type="t1"),
    )
    assert imp1.created_at is not None
    assert imp1.updated_at == imp1.created_at

    # Test case 2: created_at is None, updated_at is not
    now = datetime.now(tz=timezone.utc)
    imp2 = Import(
        id="1",
        name="n1",
        code="c1",
        relation="r1",
        identifiers=[],
        source=Source(id="s1", state={}, type="t1"),
        updated_at=now,
    )
    assert imp2.created_at == now

    # Test case 3: updated_at is None, created_at is not
    imp3 = Import(
        id="1",
        name="n1",
        code="c1",
        relation="r1",
        identifiers=[],
        source=Source(id="s1", state={}, type="t1"),
        created_at=now,
    )
    assert imp3.updated_at == now


def test_import_list_init():
    """Test the ImportList __init__ method."""
    imp = Import(
        id="1",
        name="n1",
        code="c1",
        relation="r1",
        identifiers=[],
        source=Source(id="s1", state={}, type="t1"),
    )
    imp_list = ImportList([imp])
    assert len(imp_list) == 1
    assert imp_list.id_dictionary["1"] == imp
