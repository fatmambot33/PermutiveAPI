"""Regression tests for release metadata validation."""

from __future__ import annotations

import json
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


def _write_release_fixture(root: Path, *, plugin_version: str = "1.2.3") -> None:
    """Write a minimal internally consistent release metadata fixture."""
    (root / "pyproject.toml").write_text(
        '[project]\nname = "PermutiveAPI"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )
    (root / "CHANGELOG.md").write_text(
        "# Changelog\n\n## 1.2.3 - 2026-09-13\n",
        encoding="utf-8",
    )
    releases = root / "docs" / "releases"
    releases.mkdir(parents=True)
    (releases / "1.2.3.md").write_text(
        "# PermutiveAPI 1.2.3\n",
        encoding="utf-8",
    )
    plugin_dir = root / "plugins" / "permutiveapi" / ".codex-plugin"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.json").write_text(
        json.dumps({"name": "permutiveapi", "version": plugin_version}),
        encoding="utf-8",
    )


def test_current_release_metadata_is_valid() -> None:
    """Keep the repository's current release metadata internally consistent."""
    result = _run_validator(ROOT)

    assert result.returncode == 0, result.stdout + result.stderr


def test_release_note_title_must_match_project_version(tmp_path: Path) -> None:
    """Reject release notes whose document title drifts from the project version."""
    _write_release_fixture(tmp_path)
    release_note = tmp_path / "docs" / "releases" / "1.2.3.md"
    release_note.write_text("# PermutiveAPI 1.2.2\n", encoding="utf-8")

    result = _run_validator(tmp_path)

    assert result.returncode == 1
    assert (
        "docs/releases/1.2.3.md must start with `# PermutiveAPI 1.2.3`" in result.stdout
    )


def test_codex_plugin_version_must_match_project_version(tmp_path: Path) -> None:
    """Reject release candidates whose Codex plugin version has drifted."""
    _write_release_fixture(tmp_path, plugin_version="1.2.2")

    result = _run_validator(tmp_path)

    assert result.returncode == 1
    assert (
        "Codex plugin version '1.2.2' does not match project version '1.2.3'"
        in result.stdout
    )


def test_codex_plugin_manifest_must_be_an_object(tmp_path: Path) -> None:
    """Reject syntactically valid non-object plugin manifests cleanly."""
    _write_release_fixture(tmp_path)
    plugin_manifest = (
        tmp_path / "plugins" / "permutiveapi" / ".codex-plugin" / "plugin.json"
    )
    plugin_manifest.write_text("[]\n", encoding="utf-8")

    result = _run_validator(tmp_path)

    assert result.returncode == 1
    assert (
        "invalid Codex plugin manifest: top-level JSON must be an object"
        in result.stdout
    )
