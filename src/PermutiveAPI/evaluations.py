"""Deterministic evaluations for governed AI-native execution."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Mapping, Tuple

from .ai_native import (
    AgentWorkflowRunner,
    ApprovalMode,
    ExecutionPolicy,
    GovernedToolExecutor,
    InvocationContext,
    InvocationResult,
    WorkflowStep,
)
from .tools import ToolDefinition, ToolRegistry


@dataclass(frozen=True)
class EvaluationObservation:
    """Represent one safe evaluation observation.

    Parameters
    ----------
    passed
        Whether the observed behavior matches the contract.
    detail
        Stable explanation that contains no credentials or payload secrets.
    """

    passed: bool
    detail: str


EvaluationRunner = Callable[[], EvaluationObservation]


@dataclass(frozen=True)
class EvaluationCase:
    """Define one deterministic evaluation case."""

    name: str
    category: str
    runner: EvaluationRunner


@dataclass(frozen=True)
class EvaluationResult:
    """Record the safe result of one evaluation case."""

    name: str
    category: str
    passed: bool
    detail: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return {
            "name": self.name,
            "category": self.category,
            "passed": self.passed,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class EvaluationScorecard:
    """Summarize deterministic governed-platform evaluations."""

    results: Tuple[EvaluationResult, ...]
    schema_version: int = 1

    @property
    def total(self) -> int:
        """Return the total number of evaluation cases."""
        return len(self.results)

    @property
    def passed(self) -> int:
        """Return the number of passing cases."""
        return sum(result.passed for result in self.results)

    @property
    def failed(self) -> int:
        """Return the number of failing cases."""
        return self.total - self.passed

    @property
    def ok(self) -> bool:
        """Return whether every case passed and at least one case ran."""
        return bool(self.results) and self.failed == 0

    def to_dict(self) -> dict[str, object]:
        """Return a deterministic JSON-serializable scorecard."""
        return {
            "schema_version": self.schema_version,
            "summary": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "ok": self.ok,
            },
            "results": [result.to_dict() for result in self.results],
        }

    def to_json(self) -> str:
        """Return canonical formatted JSON."""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"


def select_tool_by_tag(registry: ToolRegistry, tag: str) -> ToolDefinition:
    """Select exactly one tool for a capability tag.

    Raises
    ------
    LookupError
        If no tool or more than one tool matches the capability.
    """
    matches = registry.list(tag=tag)
    if len(matches) != 1:
        raise LookupError(
            f"Capability {tag!r} must resolve to exactly one tool; found {len(matches)}."
        )
    return matches[0]


def run_evaluations(cases: tuple[EvaluationCase, ...]) -> EvaluationScorecard:
    """Run evaluation cases without exposing exception messages."""
    results = []
    for case in cases:
        try:
            observation = case.runner()
            result = EvaluationResult(
                name=case.name,
                category=case.category,
                passed=observation.passed,
                detail=observation.detail,
            )
        except Exception as error:  # noqa: BLE001 - evaluation boundary
            result = EvaluationResult(
                name=case.name,
                category=case.category,
                passed=False,
                detail=f"Evaluation runner failed with {type(error).__name__}.",
            )
        results.append(result)
    return EvaluationScorecard(results=tuple(results))


def default_evaluation_cases() -> tuple[EvaluationCase, ...]:
    """Return the canonical deterministic platform evaluation cases."""
    return (
        EvaluationCase("tool_selection", "discovery", _tool_selection),
        EvaluationCase("unsupported_capability", "discovery", _unsupported_capability),
        EvaluationCase("read_without_approval", "policy", _read_without_approval),
        EvaluationCase("write_requires_approval", "policy", _write_requires_approval),
        EvaluationCase("allow_list_enforced", "policy", _allow_list_enforced),
        EvaluationCase("deny_list_enforced", "policy", _deny_list_enforced),
        EvaluationCase("failure_redaction", "security", _failure_redaction),
        EvaluationCase("idempotent_write", "reliability", _idempotent_write),
        EvaluationCase("workflow_bound", "workflow", _workflow_bound),
        EvaluationCase("partial_failure", "workflow", _partial_failure),
        EvaluationCase("audit_completeness", "audit", _audit_completeness),
    )


def run_default_evaluations() -> EvaluationScorecard:
    """Run the canonical deterministic platform scorecard."""
    return run_evaluations(default_evaluation_cases())


def _registry() -> tuple[ToolRegistry, dict[str, int]]:
    state = {"writes": 0}

    def read_value(value: int = 1) -> int:
        return value

    def write_value(value: int) -> Mapping[str, int]:
        state["writes"] += 1
        return {"value": value, "writes": state["writes"]}

    def fail_secret() -> None:
        raise RuntimeError("secret-token-must-not-escape")

    registry = ToolRegistry(
        (
            ToolDefinition(
                name="read_value",
                description="Read a deterministic value.",
                input_schema={"type": "object", "properties": {}},
                handler=read_value,
                tags=("read",),
            ),
            ToolDefinition(
                name="write_value",
                description="Write a deterministic value.",
                input_schema={
                    "type": "object",
                    "properties": {"value": {"type": "integer"}},
                    "required": ["value"],
                    "additionalProperties": False,
                },
                handler=write_value,
                tags=("write",),
                read_only=False,
            ),
            ToolDefinition(
                name="fail_secret",
                description="Fail with a secret-bearing exception.",
                input_schema={"type": "object", "properties": {}},
                handler=fail_secret,
                tags=("failure",),
            ),
        )
    )
    return registry, state


def _tool_selection() -> EvaluationObservation:
    registry, _ = _registry()
    selected = select_tool_by_tag(registry, "read")
    return EvaluationObservation(
        selected.name == "read_value",
        "A unique capability selects the expected read-only tool.",
    )


def _unsupported_capability() -> EvaluationObservation:
    registry, _ = _registry()
    try:
        select_tool_by_tag(registry, "unsupported")
    except LookupError:
        return EvaluationObservation(
            True,
            "Unsupported capabilities fail before tool execution.",
        )
    return EvaluationObservation(False, "Unsupported capability was accepted.")


def _read_without_approval() -> EvaluationObservation:
    registry, _ = _registry()
    result = GovernedToolExecutor(registry).invoke(
        "read_value",
        {"value": 7},
        context=InvocationContext(run_id="eval-read"),
    )
    return EvaluationObservation(
        result.ok and result.output == 7,
        "Read-only tools execute without write approval.",
    )


def _write_requires_approval() -> EvaluationObservation:
    registry, _ = _registry()
    try:
        GovernedToolExecutor(registry).invoke(
            "write_value",
            {"value": 1},
            context=InvocationContext(run_id="eval-write"),
        )
    except PermissionError:
        return EvaluationObservation(True, "Mutating tools require explicit approval.")
    return EvaluationObservation(False, "Mutating tool executed without approval.")


def _allow_list_enforced() -> EvaluationObservation:
    registry, _ = _registry()
    executor = GovernedToolExecutor(
        registry,
        policy=ExecutionPolicy(
            approval_mode=ApprovalMode.NEVER,
            allowed_tools=frozenset({"read_value"}),
        ),
    )
    try:
        executor.invoke(
            "write_value",
            {"value": 1},
            context=InvocationContext(run_id="eval-allow"),
        )
    except PermissionError:
        return EvaluationObservation(True, "Tool allow-lists are enforced.")
    return EvaluationObservation(False, "Unlisted tool executed.")


def _deny_list_enforced() -> EvaluationObservation:
    registry, _ = _registry()
    executor = GovernedToolExecutor(
        registry,
        policy=ExecutionPolicy(
            approval_mode=ApprovalMode.NEVER,
            denied_tools=frozenset({"read_value"}),
        ),
    )
    try:
        executor.invoke(
            "read_value",
            {},
            context=InvocationContext(run_id="eval-deny"),
        )
    except PermissionError:
        return EvaluationObservation(True, "Tool deny-lists are enforced.")
    return EvaluationObservation(False, "Denied tool executed.")


def _failure_redaction() -> EvaluationObservation:
    registry, _ = _registry()
    result = GovernedToolExecutor(registry).invoke(
        "fail_secret",
        {},
        context=InvocationContext(run_id="eval-redaction"),
    )
    serialized = json.dumps(result.to_dict(), sort_keys=True)
    return EvaluationObservation(
        not result.ok and "secret-token-must-not-escape" not in serialized,
        "Tool failures preserve type information without exception-message secrets.",
    )


def _idempotent_write() -> EvaluationObservation:
    registry, state = _registry()
    executor = GovernedToolExecutor(registry)
    context = InvocationContext(
        run_id="eval-idempotency",
        approved=True,
        idempotency_key="stable-evaluation-key",
    )
    first = executor.invoke("write_value", {"value": 2}, context=context)
    second = executor.invoke("write_value", {"value": 99}, context=context)
    return EvaluationObservation(
        first is second and state["writes"] == 1,
        "Repeated idempotency keys return one completed write result.",
    )


def _workflow_bound() -> EvaluationObservation:
    registry, _ = _registry()
    runner = AgentWorkflowRunner(
        GovernedToolExecutor(
            registry,
            policy=ExecutionPolicy(
                approval_mode=ApprovalMode.NEVER,
                max_steps=1,
            ),
        )
    )
    steps = (
        WorkflowStep(name="first", tool_name="read_value"),
        WorkflowStep(name="second", tool_name="read_value"),
    )
    try:
        runner.run(steps, run_id="eval-bound")
    except ValueError:
        return EvaluationObservation(True, "Workflow step limits are enforced.")
    return EvaluationObservation(False, "Oversized workflow executed.")


def _partial_failure() -> EvaluationObservation:
    registry, _ = _registry()
    runner = AgentWorkflowRunner(
        GovernedToolExecutor(
            registry,
            policy=ExecutionPolicy(approval_mode=ApprovalMode.NEVER),
        )
    )
    result = runner.run(
        (
            WorkflowStep(
                name="failure",
                tool_name="fail_secret",
                continue_on_error=True,
            ),
            WorkflowStep(name="recovery", tool_name="read_value"),
        ),
        run_id="eval-partial",
    )
    return EvaluationObservation(
        not result.ok
        and len(result.steps) == 2
        and not result.steps[0].ok
        and result.steps[1].ok,
        "Continue-on-error records partial failure and executes the next step.",
    )


def _audit_completeness() -> EvaluationObservation:
    registry, _ = _registry()
    events: list[InvocationResult] = []
    executor = GovernedToolExecutor(registry, audit_sink=events.append)
    result = executor.invoke(
        "read_value",
        {},
        context=InvocationContext(run_id="eval-audit"),
    )
    complete = (
        len(events) == 1
        and events[0] is result
        and result.tool_name == "read_value"
        and result.run_id == "eval-audit"
        and bool(result.started_at)
        and bool(result.finished_at)
    )
    return EvaluationObservation(
        complete,
        "Audit sinks receive one complete structured invocation result.",
    )


__all__ = [
    "EvaluationCase",
    "EvaluationObservation",
    "EvaluationResult",
    "EvaluationRunner",
    "EvaluationScorecard",
    "default_evaluation_cases",
    "run_default_evaluations",
    "run_evaluations",
    "select_tool_by_tag",
]
