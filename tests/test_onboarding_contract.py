"""Contracts for canonical onboarding and local credential setup."""

from __future__ import annotations

from pathlib import Path

from PermutiveAPI.cli import configure, doctor


def test_readme_promotes_only_current_canonical_onboarding() -> None:
    """The primary onboarding guide stays aligned with the supported product."""
    readme = Path("README.md").read_text(encoding="utf-8")

    required = (
        "PermutiveClient",
        "AsyncPermutiveClient",
        "PermutiveAPI[async]",
        "PermutiveAPI[dataframe]",
        "permutiveapi configure",
        "permutiveapi validate",
        "CodexPlugin",
        "PUBLIC_API.md",
        "API_COVERAGE.md",
        "MIGRATION.md",
        "ROADMAP.md",
    )
    for value in required:
        assert value in readme

    assert "PERMUTIVE_APPLICATION_CREDENTIALS" not in readme
    assert "pandas dependency is installed automatically" not in readme.lower()
    assert "PERMUTIVE_WORKSPACE_ID" not in readme


def test_configure_and_doctor_require_only_the_api_key(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Local setup asks for and validates only runtime authentication material."""
    env_file = tmp_path / ".env"
    (tmp_path / ".gitignore").write_text(".env\n", encoding="utf-8")
    monkeypatch.setattr("PermutiveAPI.cli.getpass.getpass", lambda _: "secret-key")

    assert configure(env_file) == 0
    assert env_file.read_text(encoding="utf-8") == "PERMUTIVE_API_KEY=secret-key\n"
    assert doctor(env_file) == 0


def test_doctor_accepts_existing_api_key_without_workspace_id(tmp_path: Path) -> None:
    """Legacy workspace configuration is not required for authentication."""
    env_file = tmp_path / ".env"
    (tmp_path / ".gitignore").write_text(".env\n", encoding="utf-8")
    env_file.write_text("PERMUTIVE_API_KEY=secret-key\n", encoding="utf-8")
    env_file.chmod(0o600)

    assert doctor(env_file) == 0
