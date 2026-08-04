"""Tests for the supported package-root API contract."""

from __future__ import annotations

import PermutiveAPI


CANONICAL_EXPORTS = {
    "AliasPayload",
    "AuthenticationError",
    "AuthorizationError",
    "BatchItem",
    "BatchResult",
    "ConflictError",
    "ContextPayload",
    "DecodingError",
    "EventPayload",
    "IdentityPayload",
    "JSONObject",
    "JSONScalar",
    "JSONValue",
    "NotFoundError",
    "Page",
    "PermutiveClient",
    "RateLimitError",
    "Resource",
    "RetryPolicy",
    "SDKError",
    "SegmentationPayload",
    "ServerError",
    "TransportError",
    "ValidationError",
    "execute_batch",
}

COMPATIBILITY_EXPORTS = {
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


def test_public_exports_are_fully_classified() -> None:
    """Require every package-root export to have an explicit support class."""
    classified = CANONICAL_EXPORTS | COMPATIBILITY_EXPORTS

    assert set(PermutiveAPI.__all__) == classified


def test_public_exports_are_available() -> None:
    """Require every declared public export to resolve from the package root."""
    for name in PermutiveAPI.__all__:
        assert getattr(PermutiveAPI, name) is not None


def test_public_export_names_are_unique() -> None:
    """Keep the package-root contract deterministic and unambiguous."""
    assert len(PermutiveAPI.__all__) == len(set(PermutiveAPI.__all__))
