"""Opt-in read-only live integration smoke test."""

from __future__ import annotations

import os

import pytest

from PermutiveAPI import PermutiveClient, RetryPolicy


@pytest.mark.integration
def test_live_read_only_endpoint() -> None:
    """Validate one explicitly configured live GET without storing credentials."""
    api_key = os.getenv("PERMUTIVE_API_KEY")
    path = os.getenv("PERMUTIVE_LIVE_READ_PATH")
    if not api_key or not path:
        pytest.skip("Live credentials and a read-only path are not configured.")

    with PermutiveClient(
        api_key,
        retry_policy=RetryPolicy(max_attempts=2),
    ) as client:
        payload = client.request("GET", path, params={"limit": 1})

    assert isinstance(payload, dict)
