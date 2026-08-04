"""Framework-neutral tools for agent and plugin integrations."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence, get_type_hints

JSONSchema = dict[str, Any]
ToolHandler = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    """Describe one callable exposed to agents.

    Parameters
    ----------
    name
        Stable machine-readable tool name.
    description
        Human-readable instructions for model tool selection.
    input_schema
        JSON Schema describing accepted arguments.
    handler
        Python callable implementing the operation.
    tags
        Optional capability labels used for discovery and filtering.
    read_only
        Whether calling the tool can mutate Permutive state.
    """

    name: str
    description: str
    input_schema: JSONSchema
    handler: ToolHandler
    tags: tuple[str, ...] = ()
    read_only: bool = True

    def invoke(self, arguments: Mapping[str, Any] | None = None) -> Any:
        """Invoke the tool with validated argument names."""
        values = dict(arguments or {})
        signature = inspect.signature(self.handler)
        signature.bind(**values)
        return self.handler(**values)

    def as_openai_tool(self) -> dict[str, Any]:
        """Return the OpenAI-compatible function-tool representation."""
        return {
            "type": "function",
            "name": self.name,
            "description": self.description,
            "parameters": self.input_schema,
            "strict": True,
        }


class ToolRegistry:
    """Register, discover, export, and invoke framework-neutral tools."""

    def __init__(self, tools: Sequence[ToolDefinition] = ()) -> None:
        self._tools: dict[str, ToolDefinition] = {}
        for tool in tools:
            self.register(tool)

    def register(self, tool: ToolDefinition, *, replace: bool = False) -> None:
        """Register a tool while protecting stable names from collisions."""
        if not tool.name or not tool.name.replace("_", "").isalnum():
            raise ValueError("Tool names must contain only letters, digits, and underscores.")
        if tool.name in self._tools and not replace:
            raise ValueError(f"Tool already registered: {tool.name}")
        self._tools[tool.name] = tool

    def get(self, name: str) -> ToolDefinition:
        """Return a registered tool by name."""
        try:
            return self._tools[name]
        except KeyError as exc:
            raise KeyError(f"Unknown tool: {name}") from exc

    def list(self, *, tag: str | None = None) -> tuple[ToolDefinition, ...]:
        """List tools deterministically, optionally filtered by tag."""
        tools = self._tools.values()
        if tag is not None:
            tools = (tool for tool in tools if tag in tool.tags)
        return tuple(sorted(tools, key=lambda tool: tool.name))

    def invoke(self, name: str, arguments: Mapping[str, Any] | None = None) -> Any:
        """Invoke a registered tool."""
        return self.get(name).invoke(arguments)

    def capabilities(self) -> dict[str, Any]:
        """Return a machine-readable capability summary."""
        tools = self.list()
        return {
            "agent_tools": bool(tools),
            "tool_count": len(tools),
            "read_only_tools": sum(tool.read_only for tool in tools),
            "write_tools": sum(not tool.read_only for tool in tools),
            "tags": sorted({tag for tool in tools for tag in tool.tags}),
        }

    def as_openai_tools(self) -> list[dict[str, Any]]:
        """Export all tools for OpenAI-compatible agents."""
        return [tool.as_openai_tool() for tool in self.list()]


def tool(
    *,
    name: str | None = None,
    description: str,
    input_schema: JSONSchema,
    tags: Sequence[str] = (),
    read_only: bool = True,
) -> Callable[[ToolHandler], ToolDefinition]:
    """Decorate a callable as a framework-neutral agent tool."""

    def decorate(handler: ToolHandler) -> ToolDefinition:
        get_type_hints(handler)
        return ToolDefinition(
            name=name or handler.__name__,
            description=description.strip(),
            input_schema=dict(input_schema),
            handler=handler,
            tags=tuple(tags),
            read_only=read_only,
        )

    return decorate


__all__ = ["JSONSchema", "ToolDefinition", "ToolHandler", "ToolRegistry", "tool"]
