"""Tests for actionable Codex plugin results."""

from __future__ import annotations

import json

from PermutiveAPI.credentials import LocalCredentialsProvider
from PermutiveAPI.plugins.codex import CodexPlugin
from PermutiveAPI.plugins.runtime import PluginPolicy


def test_safe_invocation_reports_unknown_tools_without_raising() -> None:
    """Unknown tools return a stable next action and no raw exception text."""
    plugin = CodexPlugin(LocalCredentialsProvider(api_key="local-test-key"))
    try:
        result = plugin.invoke_safe("missing_tool")
    finally:
        plugin.close()

    assert result["ok"] is False
    assert result["error_type"] == "KeyError"
    assert result["error_code"] == "unknown_tool"
    assert result["retryable"] is False
    assert result["recommended_action"]
    assert result["safe_context"] == {"operation": "missing_tool"}
    assert "local-test-key" not in json.dumps(result)


def test_safe_invocation_explains_missing_write_confirmation() -> None:
    """Denied writes identify the approval problem and recommended next step."""
    plugin = CodexPlugin(
        LocalCredentialsProvider(api_key="local-test-key"),
        policy=PluginPolicy(mode="read_write"),
    )
    try:
        result = plugin.invoke_safe(
            "permutive_create_cohort",
            {"payload": {"name": "demo"}},
        )
    finally:
        plugin.close()

    assert result["ok"] is False
    assert result["error_type"] == "PermissionError"
    assert result["error_code"] == "policy_denied"
    assert "approval" in str(result["recommended_action"]).lower()
    assert result["safe_context"] == {"operation": "permutive_create_cohort"}
