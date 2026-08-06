"""Tests for the source-distribution evidence contract."""

from __future__ import annotations

import subprocess
import sys
import tarfile
from pathlib import Path


def test_sdist_contains_operational_and_release_evidence(tmp_path: Path) -> None:
    """Ensure source releases retain every reproducible validation input."""
    project_root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "dist"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--sdist",
            "--no-isolation",
            "--outdir",
            str(output_dir),
        ],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )

    archives = list(output_dir.glob("*.tar.gz"))
    assert len(archives) == 1
    with tarfile.open(archives[0], "r:gz") as archive:
        names = tuple(archive.getnames())

    required_suffixes = {
        "API_COVERAGE.md",
        "CHANGELOG.md",
        "ROADMAP.md",
        "TYPING_SCOPE.json",
        "benchmarks/budgets-v1.json",
        "contracts/api-contract-v1.json",
        "contracts/api-samples-v1.json",
        "docs/OPERATIONAL_RELIABILITY.md",
        "docs/releases/6.7.0.md",
        "recordings/core-v1.json",
        "scripts/build_release_manifest.py",
        "scripts/generate_api_contracts.py",
        "scripts/validate_performance.py",
        "scripts/validate_recordings.py",
        "scripts/verify_release_manifest.py",
    }
    for suffix in required_suffixes:
        assert any(name.endswith(f"/{suffix}") for name in names), suffix
