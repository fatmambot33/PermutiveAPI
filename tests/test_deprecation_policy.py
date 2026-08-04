"""Tests for the machine-readable deprecation lifecycle."""

from __future__ import annotations

import json
from pathlib import Path

import PermutiveAPI

POLICY_PATH = Path(__file__).parents[1] / "DEPRECATIONS.json"


def _load_policy() -> dict[str, object]:
    """Return the repository deprecation policy."""
    return json.loads(POLICY_PATH.read_text(encoding="utf-8"))


def test_deprecation_policy_has_required_contract() -> None:
    """Ensure the deprecation file remains machine-readable and complete."""
    document = _load_policy()
    policy = document["policy"]

    assert isinstance(policy, dict)
    assert policy["removal_release_type"] == "major"
    assert policy["minimum_minor_releases"] >= 1
    assert set(policy["required_fields"]) == {
        "symbol",
        "replacement",
        "warning_version",
        "removal_version",
        "migration",
    }


def test_active_deprecations_are_complete_and_public() -> None:
    """Require every active entry to describe a supported public symbol."""
    document = _load_policy()
    policy = document["policy"]
    active = document["active"]

    assert isinstance(policy, dict)
    assert isinstance(active, list)
    required_fields = set(policy["required_fields"])

    for entry in active:
        assert isinstance(entry, dict)
        assert required_fields <= set(entry)
        assert entry["symbol"] in PermutiveAPI.__all__
        assert entry["replacement"]
        assert entry["migration"]
        assert entry["warning_version"] != entry["removal_version"]
