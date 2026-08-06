"""Cross-surface capability negotiation contracts."""

from __future__ import annotations

import json

import pytest

from PermutiveAPI import (
    CapabilityNegotiationError,
    CapabilityRequirement,
    PermutiveAgentKit,
    PermutiveMCPConfig,
    ToolDefinition,
    ToolRegistry,
)
from PermutiveAPI.credentials import LocalCredentialsProvider
from PermutiveAPI.plugins.codex import CodexPlugin


def _registry() -> ToolRegistry:
    return ToolRegistry(
        (
            ToolDefinition(
                "read_fixture",
                "Read a deterministic fixture.",
                {"type": "object", "properties": {}},
                lambda: {"ok": True},
                ("read",),
            ),
            ToolDefinition(
                "write_fixture",
                "Write a deterministic fixture.",
                {"type": "object", "properties": {}},
                lambda: {"ok": True},
                ("write",),
                False,
            ),
        )
    )


def test_registry_and_agentkit_negotiate_without_execution() -> None:
    """Negotiation succeeds without calling any registered handler."""
    registry = _registry()
    requirement = CapabilityRequirement(
        interfaces=("openai_tools",),
        features=("read_tools", "write_tools"),
    )

    registry_descriptor = registry.negotiate(requirement)
    kit_descriptor = PermutiveAgentKit(registry).negotiate(
        CapabilityRequirement(
            interfaces=("agent_kit",),
            features=("governed_execution", "workflow_runner"),
        )
    )

    assert registry_descriptor.tool_count == 2
    assert registry_descriptor.write_tools == 1
    assert kit_descriptor.surface == "agent_kit"


def test_mcp_descriptor_is_secret_free() -> None:
    """MCP negotiation metadata never contains bearer tokens or headers."""
    config = PermutiveMCPConfig(
        "https://mcp.example.test",
        token="secret-token",
        headers={"X-Secret": "secret-header"},
    )
    descriptor = config.negotiate(
        CapabilityRequirement(
            interfaces=("mcp_http",),
            features=("official_mcp",),
        )
    )

    serialized = json.dumps(descriptor.to_dict())
    assert descriptor.surface == "mcp"
    assert "secret-token" not in serialized
    assert "secret-header" not in serialized


def test_codex_preserves_plugin_api_and_negotiates() -> None:
    """Codex retains plugin API 1 while negotiating as version 1.0."""
    plugin = CodexPlugin(LocalCredentialsProvider(api_key="test-api-key"))
    descriptor = plugin.negotiate(
        CapabilityRequirement(
            plugin_api_version="1.0",
            interfaces=("codex_plugin",),
            features=("local_credentials", "read_tools"),
        )
    )

    assert plugin.metadata.api_version == "1"
    assert descriptor.plugin_api_version == "1.0"
    assert descriptor.read_only_tools == descriptor.tool_count
    plugin.close()


def test_missing_capability_fails_before_handler_execution() -> None:
    """Incompatible consumers fail before any tool handler can run."""
    called = False

    def handler() -> None:
        nonlocal called
        called = True

    registry = ToolRegistry(
        (
            ToolDefinition(
                "read_fixture",
                "Read a deterministic fixture.",
                {"type": "object", "properties": {}},
                handler,
            ),
        )
    )

    with pytest.raises(CapabilityNegotiationError) as captured:
        registry.negotiate(CapabilityRequirement(features=("write_tools",)))

    assert captured.value.code == "capability_missing"
    assert called is False
