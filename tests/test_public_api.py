"""Tests for the supported package-root API contract."""

from __future__ import annotations

import PermutiveAPI

EXPECTED_PUBLIC_API = {
    "Alias",
    "AliasPayload",
    "AuthenticationError",
    "AuthorizationError",
    "BatchItem",
    "BatchResult",
    "Cohort",
    "CohortList",
    "ConflictError",
    "ContextPayload",
    "ContextSegment",
    "DecodingError",
    "Event",
    "EventPayload",
    "Identity",
    "IdentityPayload",
    "Import",
    "ImportList",
    "JSONObject",
    "JSONScalar",
    "JSONValue",
    "NotFoundError",
    "Page",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveClient",
    "PermutiveRateLimitError",
    "PermutiveResourceNotFoundError",
    "PermutiveServerError",
    "RateLimitError",
    "Resource",
    "RetryPolicy",
    "SDKError",
    "Segment",
    "SegmentList",
    "Segmentation",
    "SegmentationPayload",
    "ServerError",
    "Source",
    "TransportError",
    "ValidationError",
    "Workspace",
    "WorkspaceList",
    "execute_batch",
}


def test_public_api_is_explicit_and_complete() -> None:
    """Ensure the stable package-root API is intentional and importable."""
    assert set(PermutiveAPI.__all__) == EXPECTED_PUBLIC_API

    for public_name in PermutiveAPI.__all__:
        assert hasattr(PermutiveAPI, public_name), public_name


def test_public_api_does_not_contain_duplicates() -> None:
    """Ensure duplicate exports cannot hide mistakes in the API inventory."""
    assert len(PermutiveAPI.__all__) == len(set(PermutiveAPI.__all__))
