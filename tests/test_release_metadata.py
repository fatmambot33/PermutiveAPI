"""Regression tests for release metadata validation."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts" / "validate_release_metadata.py"


def _run_validator(root: Path) -> subprocess.CompletedProcess[str]:
    """Run the release metadata validator against a repository root."""
    return subprocess.run(
        [sys.executable, str(VALIDATOR), "--root", str(root)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_current_release_metadata_is_valid() -> None:
    """Keep the repository's current release metadata internally consistent."""
    result = _run_validator(ROOT)

    assert result.returncode == 0, result.stdout + result.stderr


def test_release_note_title_must_match_project_version(tmp_path: Path) -> None:
    """Reject release notes whose document title drifts from the project version."""
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "PermutiveAPI"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )
    (tmp_path / "CHANGELOG.md").write_text(
        "# Changelog\n\n## 1.2.3 - 2026-09-13\n",
        encoding="utf-8",
    )
    releases = tmp_path / "docs" / "releases"
    releases.mkdir(parents=True)
    (releases / "1.2.3.md").write_text(
        "# PermutiveAPI 1.2.2\n",
        encoding="utf-8",
    )

    result = _run_validator(tmp_path)

    assert result.returncode == 1
    assert (
        "docs/releases/1.2.3.md must start with `# PermutiveAPI 1.2.3`"
        in result.stdout
    )
