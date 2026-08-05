# PermutiveAPI AI-Native Platform

PermutiveAPI 6.5 turns the SDK, plugin surface, and MCP configuration into one governed agent platform.

## Platform contract

The platform exposes five stable layers:

1. **Typed SDK** — deterministic Python resources and models.
2. **Tool registry** — framework-neutral JSON Schema tools for OpenAI-compatible runtimes and plugins.
3. **Governed executor** — policy checks, write approval, structured failures, audit hooks, and idempotency.
4. **Workflow runner** — bounded multi-step execution with deterministic ordering and safe stop-on-error behavior.
5. **MCP bridge** — portable configuration for Permutive's official hosted MCP server.

No model provider is required by the core package. Agents may use OpenAI, Codex, MCP clients, or any runtime capable of calling JSON Schema tools.

## Safe by default

Mutating tools require explicit approval unless the execution policy says otherwise.

```python
from PermutiveAPI import PermutiveAgentKit
from PermutiveAPI.ai_native import WorkflowStep

kit = PermutiveAgentKit(tools=registry)

result = kit.run_workflow(
    [
        WorkflowStep(
            name="inspect-workspace",
            tool_name="workspace_get",
            arguments={"workspace_id": "workspace-id"},
        ),
        WorkflowStep(
            name="create-cohort",
            tool_name="cohort_create",
            arguments={"name": "High intent"},
            approved=True,
        ),
    ],
    run_id="campaign-2026-08-05",
)
```

## Execution policy

`ExecutionPolicy` supports:

- tool allow-lists;
- explicit deny-lists;
- approval for all calls, writes only, or no calls;
- a hard maximum workflow length.

A denied or unapproved call fails before the handler is invoked.

## Structured execution results

Every governed call returns `InvocationResult` rather than leaking runtime-specific exceptions across the agent boundary. Results contain:

- tool and run identifiers;
- success state;
- output or normalized failure details;
- UTC start and finish timestamps;
- the idempotency key used for safe retry.

## Idempotency

Successful calls with the same idempotency key return the completed result without invoking the handler again. Workflow keys are derived deterministically from the run identifier, step index, tool name, and arguments.

This protects agents from duplicate writes after transport retries or model replays.

## Auditability

Pass an `audit_sink` callback to `PermutiveAgentKit` or `GovernedToolExecutor`. The callback receives each completed invocation result and can forward it to structured logs, OpenTelemetry, a data warehouse, or an approval ledger.

Secrets must never be placed in tool arguments, metadata, outputs, or audit logs.

## Machine discovery

`PermutiveAgentKit.manifest()` returns a portable manifest describing:

- available tools and JSON Schemas;
- read/write classification;
- capability tags;
- governance features;
- MCP configuration status.

Agents can inspect the manifest before planning, avoiding unsupported operations and reducing unnecessary tool calls.

## Production requirements

A production deployment should:

- use stable tool names and backward-compatible schemas;
- keep write approval enabled;
- attach a durable audit sink;
- provide a unique run ID per user goal;
- reuse idempotency keys when retrying the same action;
- set an allow-list for narrowly scoped agents;
- keep credentials in environment variables or a secret manager;
- run the complete test, type-check, documentation, and package-build gates.
