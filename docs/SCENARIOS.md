# Governed end-to-end scenarios

PermutiveAPI includes deterministic natural-language scenarios that exercise the canonical SDK, local mock HTTP server, tool registry, execution policy, workflow runner, structured results, and audit sink without a model provider or live credentials.

## Supported requests

The version 1 planner accepts these normalized requests:

- `Inspect the current workspace`
- `List available cohorts`
- `Compare the left and right segments`
- `Create a reviewed cohort named <name>`
- `Run the bounded workspace overview`
- `Run a partial failure workflow`

Unsupported language fails before tool selection or HTTP execution.

## Run one scenario

```python
from PermutiveAPI import PermutiveClient, RetryPolicy
from PermutiveAPI.scenario_fixtures import scenario_mock_routes
from PermutiveAPI.scenarios import GovernedScenarioRunner, ScenarioRequest
from PermutiveAPI.testing import MockPermutiveServer

retry = RetryPolicy(max_attempts=1)

with MockPermutiveServer(scenario_mock_routes()) as server:
    with PermutiveClient(
        "test-api-key",
        base_url=server.base_url,
        retry_policy=retry,
    ) as client:
        runner = GovernedScenarioRunner(client)
        result = runner.run(
            ScenarioRequest(
                "Inspect the current workspace",
                run_id="example-workspace",
            )
        )

assert result.ok
```

`ScenarioResult` retains the typed workflow object for local inspection. Its `to_dict()` form omits raw request text and is intended for safe structured reporting.

## Reviewed writes

Mutating scenarios are denied by default:

```python
request = ScenarioRequest(
    "Create a reviewed cohort named example",
    run_id="example-create",
)
result = runner.run(request)
assert result.error_code == "approval_required"
```

The same request requires explicit approval:

```python
approved = ScenarioRequest(
    "Create a reviewed cohort named example",
    run_id="example-create-approved",
    approved=True,
)
result = runner.run(approved)
assert result.ok
```

Replaying the same approved request through the same runner reuses the completed idempotent write result and does not issue a second HTTP mutation.

## Negative scenarios

The end-to-end contracts verify:

- unsupported intent rejection before tool execution;
- missing write approval;
- explicit tool-policy denial even after user approval;
- bounded workflow rejection before HTTP execution;
- partial failure with `continue_on_error` and a successful recovery step;
- redacted exception messages in workflow and audit results.

## Evidence

- `scenarios/fixtures-v1.json` records the synthetic HTTP fixtures.
- `scenarios/recipes.json` records canonical requests and resolved plans.
- `scripts/validate_scenario_evidence.py` prevents drift between committed evidence and runtime contracts.
- `tests/test_scenarios.py` executes positive and adversarial scenarios over the real synchronous client.

Fixture values are synthetic. Do not add production identifiers, credentials, customer data, or copied production payloads.
