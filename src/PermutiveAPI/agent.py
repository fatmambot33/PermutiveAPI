"""Agent-facing integration helpers built on the neutral tool registry."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .ai_native import (
    AgentWorkflowRunner,
    AuditSink,
    ExecutionPolicy,
    GovernedToolExecutor,
    InvocationContext,
    InvocationResult,
    WorkflowResult,
    WorkflowStep,
    platform_manifest,
)
from .capabilities import (
    CapabilityDescriptor,
    CapabilityRequirement,
    descriptor_from_registry,
)
from .mcp import PermutiveMCPConfig
from .tools import ToolRegistry


class PermutiveAgentKit:
    """Bundle tools, governance, workflows, and MCP for agent runtimes.

    Parameters
    ----------
    tools
        Framework-neutral tool registry.
    mcp
        Optional connection to Permutive's official hosted MCP server.
    policy
        Execution policy controlling access and approvals.
    audit_sink
        Optional callback receiving every completed invocation.
    """

    def __init__(
        self,
        tools: ToolRegistry | None = None,
        *,
        mcp: PermutiveMCPConfig | None = None,
        policy: ExecutionPolicy | None = None,
        audit_sink: AuditSink | None = None,
    ) -> None:
        self.tools = tools or ToolRegistry()
        self.mcp = mcp
        self.executor = GovernedToolExecutor(
            self.tools,
            policy=policy,
            audit_sink=audit_sink,
        )
        self.workflows = AgentWorkflowRunner(self.executor)

    def capabilities(self) -> dict[str, Any]:
        """Return the backward-compatible adaptive-agent summary."""
        capabilities = self.tools.capabilities()
        capabilities.update(
            {
                "official_mcp": self.mcp is not None,
                "governed_execution": True,
                "workflow_runner": True,
                "idempotency": True,
                "structured_results": True,
            }
        )
        return capabilities

    def capability_descriptor(self) -> CapabilityDescriptor:
        """Return the versioned AgentKit capability descriptor."""
        interfaces = ["agent_kit", "json_schema", "openai_tools", "tool_registry"]
        features = [
            "governed_execution",
            "idempotency",
            "structured_results",
            "workflow_runner",
        ]
        if self.mcp is not None:
            interfaces.append("mcp_client_config")
            features.append("official_mcp")
        return descriptor_from_registry(
            self.tools,
            surface="agent_kit",
            interfaces=tuple(interfaces),
            features=tuple(features),
        )

    def negotiate(
        self,
        requirement: CapabilityRequirement,
    ) -> CapabilityDescriptor:
        """Validate required capabilities before agent execution."""
        return self.capability_descriptor().negotiate(requirement)

    def manifest(self) -> dict[str, Any]:
        """Return the portable AI-native platform manifest."""
        manifest = platform_manifest(self.tools)
        manifest["mcp"] = {
            "configured": self.mcp is not None,
            "client_config": self.mcp_config(),
        }
        return manifest

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
        *,
        run_id: str = "interactive",
        actor: str = "agent",
        approved: bool = False,
        idempotency_key: str | None = None,
    ) -> InvocationResult:
        """Execute a governed local tool call returned by an agent runtime."""
        return self.executor.invoke(
            name,
            arguments,
            context=InvocationContext(
                run_id=run_id,
                actor=actor,
                approved=approved,
                idempotency_key=idempotency_key,
            ),
        )

    def run_workflow(
        self,
        steps: Sequence[WorkflowStep],
        *,
        run_id: str,
        actor: str = "agent",
    ) -> WorkflowResult:
        """Execute a bounded, governed multi-step workflow."""
        return self.workflows.run(steps, run_id=run_id, actor=actor)


__all__ = ["PermutiveAgentKit"]
