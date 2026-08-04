"""Tests for framework-neutral agent tooling."""

from __future__ import annotations

import pytest

from PermutiveAPI.agent import PermutiveAgentKit
from PermutiveAPI.mcp import PermutiveMCPConfig
from PermutiveAPI.tools import ToolDefinition, ToolRegistry, tool


SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "integer"}},
    "required": ["value"],
    "additionalProperties": False,
}


def increment(value: int) -> int:
    """Increment a value."""
    return value + 1


def test_registry_registers_exports_and_invokes_tools() -> None:
    definition = ToolDefinition(
        name="increment",
        description="Increment an integer.",
        input_schema=SCHEMA,
        handler=increment,
        tags=("test",),
    )
    registry = ToolRegistry([definition])

    assert registry.invoke("increment", {"value": 2}) == 3
    assert registry.list(tag="test") == (definition,)
    assert registry.as_openai_tools() == [
        {
            "type": "function",
            "name": "increment",
            "description": "Increment an integer.",
            "parameters": SCHEMA,
            "strict": True,
        }
    ]


def test_registry_rejects_duplicates_and_unknown_tools() -> None:
    definition = ToolDefinition("increment", "Increment.", SCHEMA, increment)
    registry = ToolRegistry([definition])

    with pytest.raises(ValueError, match="already registered"):
        registry.register(definition)
    with pytest.raises(KeyError, match="Unknown tool"):
        registry.invoke("missing")


def test_tool_decorator_creates_definition() -> None:
    @tool(description="Increment.", input_schema=SCHEMA, tags=("math",))
    def decorated(value: int) -> int:
        return value + 1

    assert decorated.name == "decorated"
    assert decorated.tags == ("math",)
    assert decorated.invoke({"value": 4}) == 5


def test_agent_kit_reports_local_and_mcp_capabilities() -> None:
    registry = ToolRegistry(
        [ToolDefinition("increment", "Increment.", SCHEMA, increment)]
    )
    mcp = PermutiveMCPConfig(url="https://example.com/mcp")
    kit = PermutiveAgentKit(registry, mcp=mcp)

    assert kit.capabilities() == {
        "agent_tools": True,
        "tool_count": 1,
        "read_only_tools": 1,
        "write_tools": 0,
        "tags": [],
        "official_mcp": True,
    }
    assert kit.invoke("increment", {"value": 8}) == 9
    assert kit.mcp_config() == mcp.to_client_config()
