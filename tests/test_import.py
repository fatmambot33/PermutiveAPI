from PermutiveAPI.Audience.Import import ImportList
from PermutiveAPI.Audience.Source import Source


def test_import_list_from_json_and_caches():
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
    imports = ImportList.from_json(data)
    assert imports.id_dictionary["1"].name == "Import1"
    assert imports.name_dictionary["Import2"].id == "2"
    assert imports.identifier_dictionary["b"][0].id == "1"
    assert isinstance(imports[0].source, Source)
