"""Deterministic governed scenarios over the canonical PermutiveAPI SDK."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional, Tuple

from .ai_native import (
    AgentWorkflowRunner,
    ExecutionPolicy,
    GovernedToolExecutor,
    InvocationResult,
    WorkflowResult,
    WorkflowStep,
)
from .client import PermutiveClient
from .sdk import JSONValue
from .tools import ToolDefinition, ToolRegistry


class ScenarioIntent(str, Enum):
    """Supported deterministic natural-language scenario intents."""

    INSPECT_WORKSPACE = "inspect_workspace"
    LIST_COHORTS = "list_cohorts"
    COMPARE_SEGMENTS = "compare_segments"
    CREATE_REVIEWED_COHORT = "create_reviewed_cohort"
    BOUNDED_OVERVIEW = "bounded_overview"
    PARTIAL_FAILURE = "partial_failure"


@dataclass(frozen=True)
class ScenarioRequest:
    """Describe one deterministic natural-language scenario request."""

    text: str
    run_id: str
    approved: bool = False


@dataclass(frozen=True)
class ScenarioPlan:
    """Record the resolved intent and governed workflow steps."""

    intent: ScenarioIntent
    steps: Tuple[WorkflowStep, ...]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible plan representation."""
        return {
            "intent": self.intent.value,
            "steps": [
                {
                    "name": step.name,
                    "tool_name": step.tool_name,
                    "arguments": dict(step.arguments),
                    "approved": step.approved,
                    "continue_on_error": step.continue_on_error,
                }
                for step in self.steps
            ],
        }


@dataclass(frozen=True)
class ScenarioResult:
    """Return one structured governed scenario outcome."""

    request: ScenarioRequest
    ok: bool
    plan: Optional[ScenarioPlan] = None
    workflow: Optional[WorkflowResult] = None
    error_code: Optional[str] = None
    detail: str = ""
    audit: Tuple[InvocationResult, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """Return a safe JSON-compatible scenario result."""
        return {
            "request": {
                "text": self.request.text,
                "run_id": self.request.run_id,
                "approved": self.request.approved,
            },
            "ok": self.ok,
            "plan": self.plan.to_dict() if self.plan is not None else None,
            "workflow": (
                self.workflow.to_dict() if self.workflow is not None else None
            ),
            "error_code": self.error_code,
            "detail": self.detail,
            "audit": [event.to_dict() for event in self.audit],
        }


class ScenarioPlanner:
    """Resolve a small supported language surface into deterministic plans."""

    def plan(self, request: ScenarioRequest) -> ScenarioPlan:
        """Resolve one request or raise for unsupported language."""
        normalized = " ".join(request.text.lower().split())
        if normalized == "inspect the current workspace":
            return ScenarioPlan(
                ScenarioIntent.INSPECT_WORKSPACE,
                (WorkflowStep("inspect", "inspect_workspace"),),
            )
        if normalized == "list available cohorts":
            return ScenarioPlan(
                ScenarioIntent.LIST_COHORTS,
                (WorkflowStep("list", "list_cohorts"),),
            )
        if normalized == "compare the left and right segments":
            return ScenarioPlan(
                ScenarioIntent.COMPARE_SEGMENTS,
                (WorkflowStep("compare", "compare_segments"),),
            )
        prefix = "create a reviewed cohort named "
        if normalized.startswith(prefix) and normalized[len(prefix) :].strip():
            name = normalized[len(prefix) :].strip()
            return ScenarioPlan(
                ScenarioIntent.CREATE_REVIEWED_COHORT,
                (
                    WorkflowStep(
                        "create",
                        "create_reviewed_cohort",
                        arguments={"name": name},
                        approved=request.approved,
                    ),
                ),
            )
        if normalized == "run the bounded workspace overview":
            return ScenarioPlan(
                ScenarioIntent.BOUNDED_OVERVIEW,
                (
                    WorkflowStep("inspect", "inspect_workspace"),
                    WorkflowStep("list", "list_cohorts"),
                ),
            )
        if normalized == "run a partial failure workflow":
            return ScenarioPlan(
                ScenarioIntent.PARTIAL_FAILURE,
                (
                    WorkflowStep(
                        "failure",
                        "fail_request",
                        continue_on_error=True,
                    ),
                    WorkflowStep("recovery", "inspect_workspace"),
                ),
            )
        raise LookupError("Unsupported scenario intent.")


class GovernedScenarioRunner:
    """Execute deterministic scenarios through governed tools and workflows."""

    def __init__(
        self,
        client: PermutiveClient,
        *,
        policy: Optional[ExecutionPolicy] = None,
        planner: Optional[ScenarioPlanner] = None,
    ) -> None:
        self.registry = scenario_tool_registry(client)
        self.audit_events: list[InvocationResult] = []
        self.executor = GovernedToolExecutor(
            self.registry,
            policy=policy,
            audit_sink=self.audit_events.append,
        )
        self.workflow_runner = AgentWorkflowRunner(self.executor)
        self.planner = planner or ScenarioPlanner()

    def run(self, request: ScenarioRequest) -> ScenarioResult:
        """Plan and execute one supported scenario safely."""
        audit_start = len(self.audit_events)
        try:
            plan = self.planner.plan(request)
        except LookupError:
            return ScenarioResult(
                request=request,
                ok=False,
                error_code="unsupported_intent",
                detail="The request does not match a supported deterministic scenario.",
            )
        try:
            workflow = self.workflow_runner.run(
                plan.steps,
                run_id=request.run_id,
                actor="scenario",
            )
        except PermissionError as error:
            message = str(error)
            code = (
                "approval_required"
                if "explicit approval" in message
                else "policy_denied"
            )
            return ScenarioResult(
                request=request,
                ok=False,
                plan=plan,
                error_code=code,
                detail=(
                    "The scenario requires explicit approval."
                    if code == "approval_required"
                    else "The scenario is not allowed by the execution policy."
                ),
                audit=tuple(self.audit_events[audit_start:]),
            )
        except ValueError:
            return ScenarioResult(
                request=request,
                ok=False,
                plan=plan,
                error_code="workflow_limit_exceeded",
                detail="The scenario exceeds the configured workflow step limit.",
                audit=tuple(self.audit_events[audit_start:]),
            )
        return ScenarioResult(
            request=request,
            ok=workflow.ok,
            plan=plan,
            workflow=workflow,
            error_code=None if workflow.ok else "workflow_failed",
            detail=(
                "The governed scenario completed successfully."
                if workflow.ok
                else "The governed scenario completed with a recorded failure."
            ),
            audit=tuple(self.audit_events[audit_start:]),
        )


def scenario_tool_registry(client: PermutiveClient) -> ToolRegistry:
    """Build canonical scenario tools over one configured client."""

    def inspect_workspace() -> JSONValue:
        return client.request("GET", "v1/workspaces/current")

    def list_cohorts() -> JSONValue:
        return client.request("GET", "v1/cohorts")

    def compare_segments() -> Mapping[str, Any]:
        left = _object(client.request("GET", "v1/segments/left"))
        right = _object(client.request("GET", "v1/segments/right"))
        left_size = _integer(left.get("size"))
        right_size = _integer(right.get("size"))
        return {
            "left": left,
            "right": right,
            "difference": left_size - right_size,
        }

    def create_reviewed_cohort(name: str) -> JSONValue:
        return client.request(
            "POST",
            "v1/cohorts/reviewed",
            json={"name": name, "reviewed": True},
        )

    def fail_request() -> JSONValue:
        return client.request("GET", "v1/server-failure")

    return ToolRegistry(
        (
            ToolDefinition(
                name="inspect_workspace",
                description="Inspect the current workspace.",
                input_schema={"type": "object", "properties": {}},
                handler=inspect_workspace,
                tags=("workspace", "read"),
            ),
            ToolDefinition(
                name="list_cohorts",
                description="List available cohorts.",
                input_schema={"type": "object", "properties": {}},
                handler=list_cohorts,
                tags=("cohorts", "read"),
            ),
            ToolDefinition(
                name="compare_segments",
                description="Compare two deterministic segment fixtures.",
                input_schema={"type": "object", "properties": {}},
                handler=compare_segments,
                tags=("segments", "read"),
            ),
            ToolDefinition(
                name="create_reviewed_cohort",
                description="Create one explicitly reviewed cohort.",
                input_schema={
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                    "additionalProperties": False,
                },
                handler=create_reviewed_cohort,
                tags=("cohorts", "write"),
                read_only=False,
            ),
            ToolDefinition(
                name="fail_request",
                description="Exercise a deterministic server failure.",
                input_schema={"type": "object", "properties": {}},
                handler=fail_request,
                tags=("failure", "read"),
            ),
        )
    )


def scenario_recipe_catalog() -> dict[str, object]:
    """Return the versioned canonical deterministic scenario recipes."""
    requests = (
        ScenarioRequest("Inspect the current workspace", "recipe-workspace"),
        ScenarioRequest("List available cohorts", "recipe-cohorts"),
        ScenarioRequest(
            "Compare the left and right segments",
            "recipe-segments",
        ),
        ScenarioRequest(
            "Create a reviewed cohort named example",
            "recipe-create",
            approved=True,
        ),
        ScenarioRequest(
            "Run the bounded workspace overview",
            "recipe-overview",
        ),
    )
    planner = ScenarioPlanner()
    return {
        "version": 1,
        "recipes": [
            {
                "request": {
                    "text": request.text,
                    "run_id": request.run_id,
                    "approved": request.approved,
                },
                "plan": planner.plan(request).to_dict(),
            }
            for request in requests
        ],
    }


def _object(value: JSONValue) -> dict[str, JSONValue]:
    if not isinstance(value, dict):
        raise TypeError("Expected a JSON object response.")
    return value


def _integer(value: Optional[JSONValue]) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("Expected an integer fixture value.")
    return value


__all__ = [
    "GovernedScenarioRunner",
    "ScenarioIntent",
    "ScenarioPlan",
    "ScenarioPlanner",
    "ScenarioRequest",
    "ScenarioResult",
    "scenario_recipe_catalog",
    "scenario_tool_registry",
]
