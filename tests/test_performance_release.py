"""Tests for performance budgets and immutable release evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from PermutiveAPI.performance import (
    PerformanceBudget,
    load_performance_budgets,
    measure_operation,
    performance_report,
)
from PermutiveAPI.release_evidence import (
    create_release_manifest,
    verify_release_manifest,
    write_release_manifest,
)


def test_performance_measurement_is_deterministic_with_injected_clock() -> None:
    """Regression summaries depend only on measured durations and budgets."""
    ticks = iter((0.0, 0.001, 0.001, 0.003, 0.003, 0.004))
    calls = []

    result = measure_operation(
        lambda: calls.append("called"),
        PerformanceBudget("example", 3, 0.01),
        clock=lambda: next(ticks),
    )

    assert result.ok is True
    assert result.median_seconds == pytest.approx(0.001)
    assert len(calls) == 4
    assert performance_report((result,))["ok"] is True


def test_committed_performance_budgets_are_complete() -> None:
    """Every canonical local operation has one positive unique budget."""
    budgets = load_performance_budgets(Path("benchmarks/budgets-v1.json"))

    assert {budget.name for budget in budgets} == {
        "contract-manifest",
        "query-serialization",
        "recipe-discovery",
        "recording-load",
    }
    assert all(budget.max_median_seconds > 0 for budget in budgets)


def test_release_manifest_verifies_exact_files_and_detects_tampering(
    tmp_path: Path,
) -> None:
    """Published files cannot change after candidate evidence is generated."""
    wheel = tmp_path / "dist" / "package.whl"
    sdist = tmp_path / "dist" / "package.tar.gz"
    sbom = tmp_path / "evidence" / "sbom.cdx.json"
    wheel.parent.mkdir()
    sbom.parent.mkdir()
    wheel.write_bytes(b"wheel")
    sdist.write_bytes(b"sdist")
    sbom.write_text("{}\n", encoding="utf-8")

    manifest = create_release_manifest(
        project="PermutiveAPI",
        version="6.7.0",
        source_commit="abc123",
        files=(wheel, sdist, sbom),
        root=tmp_path,
    )
    manifest_path = tmp_path / "evidence" / "release-manifest.json"
    write_release_manifest(manifest, manifest_path)

    verified = verify_release_manifest(manifest_path, root=tmp_path)
    assert verified["project"] == "PermutiveAPI"
    assert verified["package_version"] == "6.7.0"
    assert verified["source_commit"] == "abc123"
    assert len(verified["artifacts"]) == 3

    wheel.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="verification failed"):
        verify_release_manifest(manifest_path, root=tmp_path)


def test_release_manifest_rejects_duplicate_files(tmp_path: Path) -> None:
    """One candidate file cannot appear more than once in evidence."""
    wheel = tmp_path / "package.whl"
    wheel.write_bytes(b"wheel")

    with pytest.raises(ValueError, match="must be unique"):
        create_release_manifest(
            project="PermutiveAPI",
            version="6.7.0",
            source_commit="abc123",
            files=(wheel, wheel),
            root=tmp_path,
        )


def test_release_manifest_rejects_invalid_identity_or_digest(tmp_path: Path) -> None:
    """Verification requires complete release identity and valid SHA-256 data."""
    artifact = tmp_path / "package.whl"
    artifact.write_bytes(b"wheel")
    manifest = create_release_manifest(
        project="PermutiveAPI",
        version="6.7.0",
        source_commit="abc123",
        files=(artifact,),
        root=tmp_path,
    )
    manifest["project"] = ""
    manifest_path = tmp_path / "release-manifest.json"
    write_release_manifest(manifest, manifest_path)
    with pytest.raises(ValueError, match="non-empty string"):
        verify_release_manifest(manifest_path, root=tmp_path)

    manifest["project"] = "PermutiveAPI"
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, list)
    raw_artifact = artifacts[0]
    assert isinstance(raw_artifact, dict)
    raw_artifact["sha256"] = "not-a-digest"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="SHA-256"):
        verify_release_manifest(manifest_path, root=tmp_path)


def test_release_manifest_rejects_files_outside_root(tmp_path: Path) -> None:
    """Release evidence cannot reference files outside its candidate root."""
    outside = tmp_path.parent / "outside-release-file"
    outside.write_bytes(b"outside")
    try:
        with pytest.raises(ValueError, match="outside the evidence root"):
            create_release_manifest(
                project="PermutiveAPI",
                version="6.7.0",
                source_commit="abc123",
                files=(outside,),
                root=tmp_path,
            )
    finally:
        outside.unlink(missing_ok=True)
