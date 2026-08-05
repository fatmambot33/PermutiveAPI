"""Contract tests for the first-class Codex plugin surface."""

from __future__ import annotations

from PermutiveAPI.credentials import Credentials
from PermutiveAPI.plugins import CodexPlugin, PluginPolicy


class StaticCredentials:
    """Return deterministic credentials without reading the environment."""

    def load(self) -> Credentials:
        """Return a non-secret test credential."""
        return Credentials("test-key", "test")


def test_codex_plugin_is_read_only_by_default() -> None:
    plugin = CodexPlugin(StaticCredentials())

    names = {tool.name for tool in plugin.tools().list()}

    assert names == {
        "permutive_get_cohort",
        "permutive_get_workspace",
        "permutive_list_cohorts",
        "permutive_list_segments",
    }
    assert plugin.tools().capabilities()["write_tools"] == 0


def test_codex_plugin_can_expose_curated_write_tools() -> None:
    plugin = CodexPlugin(
        StaticCredentials(),
        policy=PluginPolicy(mode="read_write"),
    )

    names = {tool.name for tool in plugin.tools().list()}

    assert "permutive_create_cohort" in names
    assert "permutive_update_cohort" in names
    assert plugin.tools().capabilities()["write_tools"] == 2


def test_codex_plugin_requires_write_confirmation() -> None:
    plugin = CodexPlugin(
        StaticCredentials(),
        policy=PluginPolicy(mode="read_write"),
    )

    try:
        plugin.invoke("permutive_create_cohort", {"payload": {}})
    except PermissionError as exc:
        assert "explicit confirmation" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("write tool executed without confirmation")


def test_codex_plugin_supports_allow_list() -> None:
    plugin = CodexPlugin(
        StaticCredentials(),
        policy=PluginPolicy(
            allowed_tools=frozenset({"permutive_get_cohort"}),
        ),
    )

    assert [tool.name for tool in plugin.tools().list()] == ["permutive_get_cohort"]


def test_codex_plugin_validation_and_metadata_are_explicit() -> None:
    plugin = CodexPlugin(StaticCredentials())

    report = plugin.validate()

    assert report.valid
    assert "credentials" in report.checks
    assert plugin.metadata.plugin_version == "1.0"
    assert plugin.metadata.api_version == "1"
    assert plugin.agent_kit().capabilities()["tool_count"] == 4
