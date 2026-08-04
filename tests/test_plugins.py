"""Tests for plugin and local credential surfaces."""

from pathlib import Path

import pytest

from PermutiveAPI.credentials import (
    Credentials,
    CredentialsError,
    LocalCredentialsProvider,
)
from PermutiveAPI.plugins.codex import CodexPlugin


def test_local_credentials_precedence(tmp_path: Path) -> None:
    """Explicit credentials take precedence over environment and dotenv."""
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("PERMUTIVE_API_KEY=file-key\n", encoding="utf-8")
    provider = LocalCredentialsProvider(
        " explicit-key ",
        environ={"PERMUTIVE_API_KEY": "environment-key"},
        dotenv_paths=[dotenv_path],
    )

    assert provider.load() == Credentials("explicit-key", "explicit")


def test_local_credentials_load_dotenv_without_mutating_environment(
    tmp_path: Path,
) -> None:
    """Dotenv credentials are read directly and their source is retained."""
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("PERMUTIVE_API_KEY=file-key\n", encoding="utf-8")
    environment = {}

    credentials = LocalCredentialsProvider(
        environ=environment, dotenv_paths=[dotenv_path]
    ).load()

    assert credentials.api_key == "file-key"
    assert credentials.source == f"dotenv:{dotenv_path}"
    assert environment == {}
    assert "file-key" not in repr(credentials)


def test_local_credentials_raise_when_missing(tmp_path: Path) -> None:
    """Missing credentials fail with an actionable error."""
    with pytest.raises(CredentialsError, match="PERMUTIVE_API_KEY"):
        LocalCredentialsProvider(
            environ={}, dotenv_paths=[tmp_path / "missing.env"]
        ).load()


def test_codex_plugin_creates_authenticated_client() -> None:
    """The Codex plugin accepts any credentials provider contract."""

    class Provider:
        def load(self) -> Credentials:
            return Credentials("test-key", "test")

    plugin = CodexPlugin()
    client = plugin.create_client(Provider())

    assert plugin.metadata.name == "codex"
    assert "local-credentials" in plugin.metadata.capabilities
    assert client._api_key == "test-key"
