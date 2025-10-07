"""Tests for the Segmentation module."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from PermutiveAPI.Segmentation import Event, Segmentation
from PermutiveAPI.Audience.Segment import Segment, SegmentList


def test_segmentation_to_json():
    """Verify that the to_json method produces the correct payload."""
    event = Event(
        name="SlotViewable",
        time="2025-07-01T15:39:11.594Z",
        session_id="f19199e4-1654-4869-b740-703fd5bafb6f",
        view_id="d30ccfc5-c621-4ac4-a282-9a30ac864c8a",
        properties={"campaign_id": "3747123491"},
    )
    request = Segmentation(user_id="user-123", events=[event])

    expected_payload = {
        "events": [
            {
                "name": "SlotViewable",
                "time": "2025-07-01T15:39:11.594Z",
                "session_id": "f19199e4-1654-4869-b740-703fd5bafb6f",
                "view_id": "d30ccfc5-c621-4ac4-a282-9a30ac864c8a",
                "properties": {"campaign_id": "3747123491"},
            }
        ],
        "user_id": "user-123",
    }

    assert request.to_json() == expected_payload


def test_segment_list_to_pd_dataframe():
    """Ensure ``to_pd_dataframe`` converts segments into a pandas ``DataFrame``."""

    segments = SegmentList(
        [
            Segment(code="s1", name="Segment 1", import_id="import-1", id="1"),
            Segment(code="s2", name="Segment 2", import_id="import-1"),
        ]
    )

    df = segments.to_pd_dataframe()

    assert df.shape[0] == 2
    assert set(df["code"]) == {"s1", "s2"}
    assert "import_id" in df.columns


def test_segment_list_pagination_is_sequential(monkeypatch):
    """Ensure ``Segment.list`` fetches pages sequentially without overlap."""

    payloads = iter(
        [
            {
                "elements": [
                    {
                        "id": "1",
                        "name": "Segment 1",
                        "code": "s1",
                        "import_id": "import-1",
                    }
                ],
                "pagination": {"next_token": "cursor-2"},
            },
            {
                "elements": [
                    {
                        "id": "2",
                        "name": "Segment 2",
                        "code": "s2",
                        "import_id": "import-1",
                    }
                ],
                "pagination": {},
            },
        ]
    )

    in_flight = {"active": False}
    seen_tokens = []

    def fake_get(api_key, url, params=None):  # noqa: ANN001 - mirror signature
        if in_flight["active"]:
            raise AssertionError("Segment.list should not issue overlapping requests")
        in_flight["active"] = True
        try:
            token = (params or {}).get("pagination_token")
            seen_tokens.append(token)
            response = Mock()
            response.json.return_value = next(payloads)
            return response
        finally:
            in_flight["active"] = False

    monkeypatch.setattr(
        Segment,
        "_request_helper",
        SimpleNamespace(get=fake_get),
    )

    segments = Segment.list("import-1", api_key="test-key")

    assert isinstance(segments, SegmentList)
    assert [segment.id for segment in segments] == ["1", "2"]
    assert seen_tokens == [None, "cursor-2"]
