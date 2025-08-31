import json
from unittest.mock import Mock, patch
from PermutiveAPI.Audience.Import import Import, ImportList
from PermutiveAPI.Audience.Source import Source


def test_import_list_from_json_and_caches(tmp_path):
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


@patch.object(Import, "_request_helper")
def test_import_list_method(mock_request_helper):
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
    mock_request_helper.get_static.return_value = mock_response

    imports = Import.list(api_key="test-key")

    assert len(imports) == 1
    assert imports[0].name == "Import1"
    mock_request_helper.get_static.assert_called_once()
