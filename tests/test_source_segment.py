from PermutiveAPI.Audience.Source import Source
from PermutiveAPI.Audience.Segment import Segment, SegmentList


def test_source_serialization():
    source = Source(id="s1", state={"active": True}, type="typeA")
    assert Source.from_json(source.to_json()) == source


def test_segment_serialization():
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


def test_segment_list_caches():
    data = [
        {"code": "c1", "name": "Segment1", "import_id": "imp", "id": "1"},
        {"code": "c2", "name": "Segment2", "import_id": "imp", "id": "2"},
    ]
    segments = SegmentList.from_json(data)
    assert segments.id_dictionary["1"].name == "Segment1"
    assert segments.name_dictionary["Segment2"].id == "2"
    assert segments.code_dictionary["c1"].id == "1"
