"""Tests for the supported package-root API contract."""

from __future__ import annotations

import PermutiveAPI


EXPECTED_PUBLIC_API = {
    "Alias",
    "Cohort",
    "CohortList",
    "ContextSegment",
    "Event",
    "Identity",
    "Import",
    "ImportList",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveRateLimitError",
    "PermutiveResourceNotFoundError",
    "PermutiveServerError",
    "Segment",
    "SegmentList",
    "Segmentation",
    "Source",
    "Workspace",
    "WorkspaceList",
}


def test_public_api_is_explicit_and_complete() -> None:
    """Ensure the stable package-root API is intentional and importable."""
    assert set(PermutiveAPI.__all__) == EXPECTED_PUBLIC_API

    for public_name in PermutiveAPI.__all__:
        assert hasattr(PermutiveAPI, public_name), public_name


def test_public_api_does_not_contain_duplicates() -> None:
    """Ensure duplicate exports cannot hide mistakes in the API inventory."""
    assert len(PermutiveAPI.__all__) == len(set(PermutiveAPI.__all__))
