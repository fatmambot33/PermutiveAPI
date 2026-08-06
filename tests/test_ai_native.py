"""Tests for governed AI-native execution primitives."""

from __future__ import annotations

from PermutiveAPI.ai_native import (
    AgentWorkflowRunner,
    ApprovalMode,
    ExecutionPolicy,
    GovernedToolExecutor,
    InvocationContext,
    WorkflowStep,
    platform_manifest,
)
from PermutiveAPI.tools import ToolDefinition, ToolRegistry


def _registry() -> ToolRegistry:
    state = {"writes": 0}

    def read_value(value: int = 1) -> int:
        return value

    def write_value(value: int) -> dict[str, int]:
        state["writes"] += 1
        return {"value": value, "writes": state["writes"]}

    return ToolRegistry(
        [
            ToolDefinition(
                name="read_value",
                description="Read a value.",
                input_schema={"type": "object", "properties": {}},
                handler=read_value,
                tags=("read",),
            ),
            ToolDefinition(
                name="write_value",
                description="Write a value.",
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
        ]
    )


def test_write_tools_require_approval_by_default() -> None:
    executor = GovernedToolExecutor(_registry())

    try:
        executor.invoke(
            "write_value",
            {"value": 3},
            context=InvocationContext(run_id="run-1"),
        )
    except PermissionError as exc:
        assert "explicit approval" in str(exc)
    else:
        raise AssertionError("Expected write approval to be enforced.")


def test_idempotency_returns_completed_result_without_duplicate_write() -> None:
    executor = GovernedToolExecutor(_registry())
    context = InvocationContext(
        run_id="run-2",
        approved=True,
        idempotency_key="stable-key",
    )

    first = executor.invoke("write_value", {"value": 4}, context=context)
    second = executor.invoke("write_value", {"value": 99}, context=context)

    assert first is second
    assert first.output == {"value": 4, "writes": 1}


def test_workflow_stops_after_failure() -> None:
    registry = _registry()

    def fail() -> None:
        raise RuntimeError("boom")

    registry.register(
        ToolDefinition(
            name="fail",
            description="Fail deliberately.",
            input_schema={"type": "object", "properties": {}},
            handler=fail,
        )
    )
    runner = AgentWorkflowRunner(
        GovernedToolExecutor(
            registry,
            policy=ExecutionPolicy(approval_mode=ApprovalMode.NEVER),
        )
    )

    result = runner.run(
        [
            WorkflowStep(name="first", tool_name="read_value"),
            WorkflowStep(name="failure", tool_name="fail"),
            WorkflowStep(name="never", tool_name="read_value"),
        ],
        run_id="run-3",
    )

    assert result.ok is False
    assert len(result.steps) == 2
    assert result.steps[-1].error_type == "RuntimeError"


def test_governed_failures_do_not_expose_exception_messages() -> None:
    """Structured failures retain the type without payload-derived secret text."""
    registry = _registry()

    def fail() -> None:
        raise RuntimeError("secret-token-must-not-escape")

    registry.register(
        ToolDefinition(
            name="fail_secret",
            description="Fail with secret-bearing text.",
            input_schema={"type": "object", "properties": {}},
            handler=fail,
        )
    )

    result = GovernedToolExecutor(registry).invoke(
        "fail_secret",
        {},
        context=InvocationContext(run_id="run-redaction"),
    )

    assert result.ok is False
    assert result.error_type == "RuntimeError"
    assert result.error_message == "Tool execution failed with RuntimeError."
    assert "secret-token-must-not-escape" not in str(result.to_dict())


def test_manifest_is_machine_readable_and_governed() -> None:
    manifest = platform_manifest(_registry())

    assert manifest["capabilities"]["tool_count"] == 2
    assert manifest["governance"]["idempotency"] is True
    assert {tool["name"] for tool in manifest["tools"]} == {
        "read_value",
        "write_value",
    }
