"""Tests for the Segmentation module."""

from PermutiveAPI.Segmentation import Event, Segmentation


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
