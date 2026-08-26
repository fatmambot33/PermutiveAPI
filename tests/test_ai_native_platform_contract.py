"""Regression tests for the vendored AI-native platform contract."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts import validate_ai_native_platform as validator


def _validate_security_evidence(
    tmp_path: Path,
    monkeypatch,
    evidence_key: str,
) -> list[str]:
    """Validate a minimal manifest using one security evidence key."""
    evidence = tmp_path / "evidence"
    evidence.write_text("repository evidence\n", encoding="utf-8")
    manifest = tmp_path / "AI_NATIVE_PLATFORM.yaml"
    schema = tmp_path / "schema.json"
    data = {
        "standard": {"ref": "v0.2.0"},
        "quality": {"security_scan": True},
        "agent": {"guarantees": sorted(validator.REQUIRED_GUARANTEES)},
        "evidence": {
            "paths": {
                "readme": "evidence",
                "tests": "evidence",
                "agent_instructions": "evidence",
                "typing": "evidence",
                "ci": "evidence",
                evidence_key: "evidence",
            }
        },
    }
    manifest.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    schema.write_text(json.dumps({"type": "object"}), encoding="utf-8")
    monkeypatch.setattr(validator, "ROOT", tmp_path)
    monkeypatch.setattr(validator, "MANIFEST", manifest)
    monkeypatch.setattr(validator, "SCHEMA", schema)
    return validator.validate()


def test_security_evidence_is_accepted(tmp_path: Path, monkeypatch) -> None:
    """Accept the canonical generic security evidence key."""
    assert _validate_security_evidence(tmp_path, monkeypatch, "security_evidence") == []


def test_security_workflow_is_not_a_compatibility_alias(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Reject the removed workflow-specific key as security evidence."""
    errors = _validate_security_evidence(tmp_path, monkeypatch, "security_workflow")

    assert "missing evidence declaration: security_evidence" in errors
