"""Tests for the official Permutive MCP configuration helpers."""

from __future__ import annotations

import json

import pytest

from PermutiveAPI import PermutiveMCPConfig


def test_builds_portable_client_configuration() -> None:
    """Configuration should use the portable HTTP MCP shape."""
    config = PermutiveMCPConfig(
        url="https://mcp.example.permutive.com/mcp",
        token="secret",
    )

    assert config.to_client_config() == {
        "mcpServers": {
            "permutive": {
                "type": "http",
                "url": "https://mcp.example.permutive.com/mcp",
                "headers": {"Authorization": "Bearer secret"},
            }
        }
    }


def test_loads_configuration_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Environment loading should support the standard variable names."""
    monkeypatch.setenv("PERMUTIVE_MCP_URL", "https://mcp.example.com/mcp")
    monkeypatch.setenv("PERMUTIVE_MCP_TOKEN", "token")

    config = PermutiveMCPConfig.from_env()

    assert config.url == "https://mcp.example.com/mcp"
    assert config.resolved_headers() == {"Authorization": "Bearer token"}


def test_rejects_insecure_or_relative_urls() -> None:
    """MCP credentials must never be sent over an insecure endpoint."""
    with pytest.raises(ValueError, match="absolute HTTPS"):
        PermutiveMCPConfig(url="http://example.com/mcp")

    with pytest.raises(ValueError, match="absolute HTTPS"):
        PermutiveMCPConfig(url="/mcp")


def test_rejects_duplicate_authorization_configuration() -> None:
    """Authentication should have one unambiguous source."""
    config = PermutiveMCPConfig(
        url="https://example.com/mcp",
        token="token",
        headers={"authorization": "Custom token"},
    )

    with pytest.raises(ValueError, match="either token or Authorization"):
        config.resolved_headers()


def test_token_is_not_exposed_in_repr() -> None:
    """Diagnostic output must not leak bearer credentials."""
    config = PermutiveMCPConfig(
        url="https://example.com/mcp",
        token="top-secret",
    )

    assert "top-secret" not in repr(config)


def test_json_output_is_valid_and_deterministic() -> None:
    """Generated configuration should be directly consumable as JSON."""
    config = PermutiveMCPConfig(url="https://example.com/mcp")

    payload = json.loads(config.to_json())

    assert payload == config.to_client_config()


def test_missing_environment_url_fails_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing endpoint should produce an actionable configuration error."""
    monkeypatch.delenv("PERMUTIVE_MCP_URL", raising=False)

    with pytest.raises(ValueError, match="PERMUTIVE_MCP_URL"):
        PermutiveMCPConfig.from_env()
