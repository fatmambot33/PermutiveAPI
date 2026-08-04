"""Agent-facing integration helpers built on the neutral tool registry."""

from __future__ import annotations

from typing import Any, Mapping

from .mcp import PermutiveMCPConfig
from .tools import ToolRegistry


class PermutiveAgentKit:
    """Bundle SDK tools and hosted MCP configuration for agent runtimes.

    Parameters
    ----------
    tools
        Framework-neutral tool registry.
    mcp
        Optional connection to Permutive's official hosted MCP server.
    """

    def __init__(
        self,
        tools: ToolRegistry | None = None,
        *,
        mcp: PermutiveMCPConfig | None = None,
    ) -> None:
        self.tools = tools or ToolRegistry()
        self.mcp = mcp

    def capabilities(self) -> dict[str, Any]:
        """Return machine-readable capabilities for adaptive agents."""
        capabilities = self.tools.capabilities()
        capabilities["official_mcp"] = self.mcp is not None
        return capabilities

    def openai_tools(self) -> list[dict[str, Any]]:
        """Export local tools in OpenAI-compatible function format."""
        return self.tools.as_openai_tools()

    def mcp_config(self) -> dict[str, object] | None:
        """Return the hosted MCP client fragment when configured."""
        return None if self.mcp is None else self.mcp.to_client_config()

    def invoke(
        self,
        name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> Any:
        """Execute a local tool call returned by an agent runtime."""
        return self.tools.invoke(name, arguments)


__all__ = ["PermutiveAgentKit"]
