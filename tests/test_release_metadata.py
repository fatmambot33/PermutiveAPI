"""Regression tests for release metadata alignment."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _project_version() -> str:
    """Return the authoritative project version from ``pyproject.toml``."""
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', pyproject, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_current_release_metadata_matches_project_version() -> None:
    """Keep release documentation aligned with the package version."""
    version = _project_version()
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    release_notes = ROOT / "docs" / "releases" / f"{version}.md"

    assert re.search(rf"^## {re.escape(version)} - ", changelog, flags=re.MULTILINE)
    assert release_notes.is_file()
    assert release_notes.read_text(encoding="utf-8").startswith(
        f"# PermutiveAPI {version}\n"
    )
