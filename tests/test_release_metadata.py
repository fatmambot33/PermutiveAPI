"""Regression tests for release metadata validation."""

from __future__ import annotations

from pathlib import Path

from scripts.validate_release_metadata import validate


ROOT = Path(__file__).resolve().parents[1]


def test_current_release_metadata_is_valid() -> None:
    """Keep the repository's current release metadata internally consistent."""
    assert validate(ROOT) == []


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

    assert validate(tmp_path) == [
        "docs/releases/1.2.3.md must start with `# PermutiveAPI 1.2.3`"
    ]
