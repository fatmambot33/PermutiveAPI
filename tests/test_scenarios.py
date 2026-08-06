"""End-to-end contracts for deterministic governed scenarios."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator, Tuple

import pytest

from PermutiveAPI import PermutiveClient, RetryPolicy
from PermutiveAPI.ai_native import ApprovalMode, ExecutionPolicy
from PermutiveAPI.scenario_fixtures import (
    scenario_fixture_catalog,
    scenario_mock_routes,
)
from PermutiveAPI.scenarios import (
    GovernedScenarioRunner,
    ScenarioRequest,
    scenario_recipe_catalog,
)
from PermutiveAPI.testing import MockPermutiveServer


@pytest.fixture
def scenario_client() -> Iterator[Tuple[MockPermutiveServer, PermutiveClient]]:
    """Yield a canonical client connected to deterministic scenario fixtures."""
    retry = RetryPolicy(
        max_attempts=1,
        initial_delay=0.001,
        multiplier=1.0,
        max_delay=0.001,
        jitter=0.0,
    )
    with MockPermutiveServer(scenario_mock_routes()) as server:
        with PermutiveClient(
            "scenario-api-key",
            base_url=server.base_url,
            retry_policy=retry,
        ) as client:
            yield server, client


def test_scenario_evidence_matches_committed_catalogs() -> None:
    """Versioned routes and recipes cannot drift from runtime contracts."""
    fixtures = json.loads(
        Path("scenarios/fixtures-v1.json").read_text(encoding="utf-8")
    )
    recipes = json.loads(Path("scenarios/recipes.json").read_text(encoding="utf-8"))

    assert fixtures == scenario_fixture_catalog()
    assert recipes == scenario_recipe_catalog()


def test_read_and_bounded_scenarios_use_expected_tools(
    scenario_client: Tuple[MockPermutiveServer, PermutiveClient],
) -> None:
    """Natural-language reads resolve to deterministic governed workflows."""
    server, client = scenario_client
    runner = GovernedScenarioRunner(client)

    workspace = runner.run(
        ScenarioRequest("Inspect the current workspace", "scenario-workspace")
    )
    cohorts = runner.run(ScenarioRequest("List available cohorts", "scenario-cohorts"))
    segments = runner.run(
        ScenarioRequest(
            "Compare the left and right segments",
            "scenario-segments",
        )
    )
    overview = runner.run(
        ScenarioRequest(
            "Run the bounded workspace overview",
            "scenario-overview",
        )
    )

    assert workspace.ok is True
    assert workspace.plan is not None
    assert workspace.plan.steps[0].tool_name == "inspect_workspace"
    assert workspace.workflow is not None
    assert workspace.workflow.steps[0].output == {
        "id": "workspace-fixture",
        "name": "Fixture Workspace",
    }
    assert cohorts.ok is True
    assert cohorts.workflow is not None
    assert cohorts.workflow.steps[0].output == {
        "items": [
            {"id": "cohort-alpha", "name": "Alpha"},
            {"id": "cohort-beta", "name": "Beta"},
        ]
    }
    assert segments.ok is True
    assert segments.workflow is not None
    assert segments.workflow.steps[0].output["difference"] == 40
    assert overview.ok is True
    assert overview.workflow is not None
    assert [step.tool_name for step in overview.workflow.steps] == [
        "inspect_workspace",
        "list_cohorts",
    ]
    assert len(overview.audit) == 2
    assert [request.path for request in server.requests] == [
        "/v1/workspaces/current",
        "/v1/cohorts",
        "/v1/segments/left",
        "/v1/segments/right",
        "/v1/workspaces/current",
        "/v1/cohorts",
    ]


def test_reviewed_write_requires_approval_and_is_idempotent(
    scenario_client: Tuple[MockPermutiveServer, PermutiveClient],
) -> None:
    """Reviewed writes require approval and do not duplicate on replay."""
    server, client = scenario_client
    runner = GovernedScenarioRunner(client)
    denied = runner.run(
        ScenarioRequest(
            "Create a reviewed cohort named Example",
            "scenario-create-denied",
        )
    )

    assert denied.ok is False
    assert denied.error_code == "approval_required"
    assert server.requests == ()

    approved_request = ScenarioRequest(
        "Create a reviewed cohort named Example",
        "scenario-create-approved",
        approved=True,
    )
    first = runner.run(approved_request)
    second = runner.run(approved_request)

    assert first.ok is True
    assert second.ok is True
    assert first.workflow is not None
    assert second.workflow is not None
    assert first.workflow.steps[0] is second.workflow.steps[0]
    assert len(server.requests) == 1
    assert server.requests[0].path == "/v1/cohorts/reviewed"
    assert server.requests[0].body == {"name": "example", "reviewed": True}
    assert len(first.audit) == 1
    assert second.audit == ()


def test_policy_denial_prevents_mutation(
    scenario_client: Tuple[MockPermutiveServer, PermutiveClient],
) -> None:
    """Explicit policy denial wins even when the user approved the write."""
    server, client = scenario_client
    runner = GovernedScenarioRunner(
        client,
        policy=ExecutionPolicy(
            approval_mode=ApprovalMode.NEVER,
            denied_tools=frozenset({"create_reviewed_cohort"}),
        ),
    )

    result = runner.run(
        ScenarioRequest(
            "Create a reviewed cohort named blocked",
            "scenario-policy-denied",
            approved=True,
        )
    )

    assert result.ok is False
    assert result.error_code == "policy_denied"
    assert server.requests == ()


def test_unsupported_intent_fails_before_tool_execution(
    scenario_client: Tuple[MockPermutiveServer, PermutiveClient],
) -> None:
    """Unknown language cannot select or execute a tool."""
    server, client = scenario_client
    result = GovernedScenarioRunner(client).run(
        ScenarioRequest("Delete every workspace", "scenario-unsupported")
    )

    assert result.ok is False
    assert result.plan is None
    assert result.error_code == "unsupported_intent"
    assert result.audit == ()
    assert server.requests == ()


def test_partial_failure_records_failure_and_recovery(
    scenario_client: Tuple[MockPermutiveServer, PermutiveClient],
) -> None:
    """Continue-on-error preserves failure evidence and completes recovery."""
    server, client = scenario_client
    result = GovernedScenarioRunner(
        client,
        policy=ExecutionPolicy(approval_mode=ApprovalMode.NEVER),
    ).run(
        ScenarioRequest(
            "Run a partial failure workflow",
            "scenario-partial",
        )
    )

    assert result.ok is False
    assert result.error_code == "workflow_failed"
    assert result.workflow is not None
    assert len(result.workflow.steps) == 2
    assert result.workflow.steps[0].ok is False
    assert result.workflow.steps[0].error_type == "ServerError"
    assert result.workflow.steps[0].error_message == (
        "Tool execution failed with ServerError."
    )
    assert result.workflow.steps[1].ok is True
    assert len(result.audit) == 2
    assert "server_failure" not in json.dumps(result.to_dict())
    assert [request.path for request in server.requests] == [
        "/v1/server-failure",
        "/v1/workspaces/current",
    ]


def test_workflow_limit_fails_before_http_execution(
    scenario_client: Tuple[MockPermutiveServer, PermutiveClient],
) -> None:
    """Oversized plans fail before the first fixture request."""
    server, client = scenario_client
    runner = GovernedScenarioRunner(
        client,
        policy=ExecutionPolicy(
            approval_mode=ApprovalMode.NEVER,
            max_steps=1,
        ),
    )

    result = runner.run(
        ScenarioRequest(
            "Run the bounded workspace overview",
            "scenario-limit",
        )
    )

    assert result.ok is False
    assert result.error_code == "workflow_limit_exceeded"
    assert result.workflow is None
    assert result.audit == ()
    assert server.requests == ()
