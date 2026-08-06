"""Governed execution primitives for AI-native Permutive workflows."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping, Sequence

from .tools import ToolDefinition, ToolRegistry


class ApprovalMode(str, Enum):
    """Control whether mutating tools require explicit approval."""

    NEVER = "never"
    WRITES = "writes"
    ALWAYS = "always"


@dataclass(frozen=True)
class ExecutionPolicy:
    """Define safety and governance rules for agent tool execution.

    Parameters
    ----------
    approval_mode
        Approval requirement applied before tool execution.
    allowed_tools
        Optional allow-list of stable tool names.
    denied_tools
        Explicit deny-list evaluated before the allow-list.
    max_steps
        Maximum number of workflow steps per run.
    """

    approval_mode: ApprovalMode = ApprovalMode.WRITES
    allowed_tools: frozenset[str] | None = None
    denied_tools: frozenset[str] = field(default_factory=frozenset)
    max_steps: int = 25

    def __post_init__(self) -> None:
        if self.max_steps < 1:
            raise ValueError("max_steps must be at least 1.")
        if self.allowed_tools is not None and self.denied_tools & self.allowed_tools:
            raise ValueError("A tool cannot be both allowed and denied.")

    def check(self, tool: ToolDefinition, *, approved: bool) -> None:
        """Validate whether a tool may execute under this policy."""
        if tool.name in self.denied_tools:
            raise PermissionError(f"Tool is denied by policy: {tool.name}")
        if self.allowed_tools is not None and tool.name not in self.allowed_tools:
            raise PermissionError(f"Tool is not allowed by policy: {tool.name}")

        requires_approval = self.approval_mode is ApprovalMode.ALWAYS or (
            self.approval_mode is ApprovalMode.WRITES and not tool.read_only
        )
        if requires_approval and not approved:
            raise PermissionError(f"Tool requires explicit approval: {tool.name}")


@dataclass(frozen=True)
class InvocationContext:
    """Carry execution metadata across agent and workflow calls."""

    run_id: str
    actor: str = "agent"
    approved: bool = False
    idempotency_key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InvocationResult:
    """Return a structured, auditable result from a tool invocation."""

    tool_name: str
    run_id: str
    ok: bool
    output: Any = None
    error_type: str | None = None
    error_message: str | None = None
    started_at: str = ""
    finished_at: str = ""
    idempotency_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "tool_name": self.tool_name,
            "run_id": self.run_id,
            "ok": self.ok,
            "output": self.output,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "idempotency_key": self.idempotency_key,
        }


AuditSink = Callable[[InvocationResult], None]


class GovernedToolExecutor:
    """Execute registered tools with policy, audit, and idempotency controls."""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        policy: ExecutionPolicy | None = None,
        audit_sink: AuditSink | None = None,
    ) -> None:
        self.registry = registry
        self.policy = policy or ExecutionPolicy()
        self.audit_sink = audit_sink
        self._completed: dict[str, InvocationResult] = {}

    def invoke(
        self,
        name: str,
        arguments: Mapping[str, Any] | None,
        *,
        context: InvocationContext,
    ) -> InvocationResult:
        """Execute one tool and always return a structured result."""
        tool = self.registry.get(name)
        self.policy.check(tool, approved=context.approved)

        key = context.idempotency_key
        if key is not None and key in self._completed:
            return self._completed[key]

        started_at = datetime.now(timezone.utc).isoformat()
        try:
            output = tool.invoke(arguments)
            result = InvocationResult(
                tool_name=name,
                run_id=context.run_id,
                ok=True,
                output=output,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                idempotency_key=key,
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts failures to data
            result = InvocationResult(
                tool_name=name,
                run_id=context.run_id,
                ok=False,
                error_type=type(exc).__name__,
                error_message=f"Tool execution failed with {type(exc).__name__}.",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                idempotency_key=key,
            )

        if key is not None and result.ok:
            self._completed[key] = result
        if self.audit_sink is not None:
            self.audit_sink(result)
        return result


@dataclass(frozen=True)
class WorkflowStep:
    """Describe one deterministic tool call in an agent workflow."""

    name: str
    tool_name: str
    arguments: Mapping[str, Any] = field(default_factory=dict)
    approved: bool = False
    continue_on_error: bool = False


@dataclass(frozen=True)
class WorkflowResult:
    """Summarize a completed governed workflow run."""

    run_id: str
    ok: bool
    steps: tuple[InvocationResult, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable workflow result."""
        return {
            "run_id": self.run_id,
            "ok": self.ok,
            "steps": [step.to_dict() for step in self.steps],
        }


class AgentWorkflowRunner:
    """Run bounded, deterministic workflows over the governed tool surface."""

    def __init__(self, executor: GovernedToolExecutor) -> None:
        self.executor = executor

    def run(
        self,
        steps: Sequence[WorkflowStep],
        *,
        run_id: str,
        actor: str = "agent",
    ) -> WorkflowResult:
        """Execute workflow steps in order and stop safely on failure."""
        if len(steps) > self.executor.policy.max_steps:
            raise ValueError("Workflow exceeds the configured max_steps limit.")

        results: list[InvocationResult] = []
        for index, step in enumerate(steps):
            key = _workflow_idempotency_key(run_id, index, step)
            result = self.executor.invoke(
                step.tool_name,
                step.arguments,
                context=InvocationContext(
                    run_id=run_id,
                    actor=actor,
                    approved=step.approved,
                    idempotency_key=key,
                    metadata={"step": step.name, "index": index},
                ),
            )
            results.append(result)
            if not result.ok and not step.continue_on_error:
                break

        return WorkflowResult(
            run_id=run_id,
            ok=bool(results) and all(result.ok for result in results),
            steps=tuple(results),
        )


def platform_manifest(
    registry: ToolRegistry,
    *,
    name: str = "PermutiveAPI",
    version: str = "1",
) -> dict[str, Any]:
    """Build a portable machine-readable AI platform manifest."""
    return {
        "name": name,
        "version": version,
        "capabilities": registry.capabilities(),
        "tools": [
            {
                "name": item.name,
                "description": item.description,
                "input_schema": item.input_schema,
                "tags": list(item.tags),
                "read_only": item.read_only,
            }
            for item in registry.list()
        ],
        "governance": {
            "approval_modes": [mode.value for mode in ApprovalMode],
            "idempotency": True,
            "structured_results": True,
            "bounded_workflows": True,
            "audit_sink": True,
        },
    }


def _workflow_idempotency_key(
    run_id: str,
    index: int,
    step: WorkflowStep,
) -> str:
    payload = json.dumps(
        {
            "run_id": run_id,
            "index": index,
            "name": step.name,
            "tool_name": step.tool_name,
            "arguments": step.arguments,
        },
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = [
    "AgentWorkflowRunner",
    "ApprovalMode",
    "AuditSink",
    "ExecutionPolicy",
    "GovernedToolExecutor",
    "InvocationContext",
    "InvocationResult",
    "WorkflowResult",
    "WorkflowStep",
    "platform_manifest",
]
