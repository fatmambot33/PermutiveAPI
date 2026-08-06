# Capability negotiation

PermutiveAPI exposes one versioned capability contract across the framework-neutral tool registry, `PermutiveAgentKit`, the Codex plugin, and official MCP client configuration.

## Why negotiate

Adaptive consumers should verify compatibility before selecting or invoking tools. Negotiation is deterministic, credential-free, and does not execute handlers or make network requests.

```python
from PermutiveAPI import CapabilityRequirement, ToolRegistry

registry = ToolRegistry()
descriptor = registry.negotiate(
    CapabilityRequirement(interfaces=("tool_registry",))
)
print(descriptor.to_dict())
```

## Compatibility rules

- Capability and tool-schema versions use `major.minor`.
- Major versions must match.
- The available minor version must be equal to or newer than the required minor.
- Every required interface and feature must be present.
- Plugin requirements use the same rule. The historic Codex plugin API value remains `"1"`; its negotiation representation is `"1.0"`.

## Stable failures

Negotiation raises `CapabilityNegotiationError` with one of these codes:

- `capability_contract_incompatible`
- `tool_schema_incompatible`
- `plugin_api_incompatible`
- `capability_missing`

Every failure includes a safe detail, a recommended action, and any missing capability names. It never includes credentials, MCP headers, payloads, or exception text from a tool handler.

## Surfaces

`ToolRegistry` describes JSON Schema and OpenAI-compatible function tools.

`PermutiveAgentKit` adds governed execution, workflows, structured results, idempotency, and optional MCP composition.

`CodexPlugin` adds the plugin interface, local credential resolution, and read/write policy metadata.

`PermutiveMCPConfig` describes only the secret-free hosted MCP composition surface. Tokens and custom header values are intentionally absent.

The machine-readable contract is committed at `capabilities/contract-v1.json` and validated in both required CI workflows.
