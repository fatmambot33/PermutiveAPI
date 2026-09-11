"""Contract tests for the distributable Codex plugin package."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PLUGIN_ROOT = ROOT / "plugins" / "permutiveapi"


def _project_version() -> str:
    """Return the project version without adding a TOML parser dependency."""
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', pyproject, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_codex_plugin_version_matches_package_version() -> None:
    manifest = json.loads(
        (PLUGIN_ROOT / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8")
    )

    assert manifest["version"] == _project_version()


def test_codex_skill_bootstraps_stable_pypi_package() -> None:
    skill = (PLUGIN_ROOT / "skills" / "permutiveapi" / "SKILL.md").read_text(
        encoding="utf-8"
    )

    assert "python -m pip install --upgrade PermutiveAPI" in skill
    assert "git+https://github.com/fatmambot33/PermutiveAPI.git" not in skill
    assert "Do not require users to write Python" in skill
    assert "explicit user confirmation" in skill
    assert "permutiveapi check --json" in skill


def test_codex_plugin_packages_welcome_and_troubleshooting_skills() -> None:
    welcome = (PLUGIN_ROOT / "skills" / "welcome" / "SKILL.md").read_text(
        encoding="utf-8"
    )
    troubleshooting = (
        PLUGIN_ROOT / "skills" / "troubleshooting" / "SKILL.md"
    ).read_text(encoding="utf-8")

    assert "permutiveapi check --json" in welcome
    assert "credentials_missing" in welcome
    assert "permutiveapi check --json" in troubleshooting
    assert "authentication_failed" in troubleshooting
    assert "authorization_denied" in troubleshooting
    assert "Never request, print, echo, upload, or log the API key" in troubleshooting


def test_codex_manifest_promotes_direct_setup() -> None:
    manifest = json.loads(
        (PLUGIN_ROOT / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8")
    )

    assert manifest["interface"]["defaultPrompt"][0] == "Set up PermutiveAPI."
    assert "stable-PyPI bootstrap" in manifest["interface"]["longDescription"]
    assert "Check my Permutive connection and credentials." in manifest["interface"][
        "defaultPrompt"
    ]
