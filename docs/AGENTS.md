# Agent and Plugin Integration

PermutiveAPI uses one framework-neutral tool layer for agents, plugins, and custom runtimes.

## Architecture

- `PermutiveAPI.tools` defines stable tool metadata, discovery, invocation, and exports.
- `PermutiveAPI.agent` combines local SDK tools with the official hosted Permutive MCP connection.
- `PermutiveAPI.mcp` configures the hosted MCP server without duplicating its tools.
- `PermutiveAPI.plugins` remains the extension mechanism for runtime-specific adapters.

The REST SDK remains the deterministic application interface. Agent tools are thin, explicit adapters over SDK operations. Hosted MCP tools remain owned and versioned by Permutive.

## Define a tool

```python
from PermutiveAPI.tools import ToolRegistry, tool


@tool(
    description="Return one cohort by identifier.",
    input_schema={
        "type": "object",
        "properties": {"cohort_id": {"type": "string"}},
        "required": ["cohort_id"],
        "additionalProperties": False,
    },
    tags=("cohorts", "read"),
)
def get_cohort(cohort_id: str) -> dict[str, object]:
    return {"id": cohort_id}


registry = ToolRegistry([get_cohort])
```

## Use with an agent runtime

```python
from PermutiveAPI.agent import PermutiveAgentKit
from PermutiveAPI.mcp import PermutiveMCPConfig

kit = PermutiveAgentKit(
    registry,
    mcp=PermutiveMCPConfig.from_env(),
)

function_tools = kit.openai_tools()
hosted_mcp = kit.mcp_config()
result = kit.invoke("get_cohort", {"cohort_id": "cohort-123"})
```

## Contract

Tool names are stable public API. Inputs use strict JSON Schema. Tool handlers validate argument names through their Python signatures. Read and write capabilities are explicitly labelled so agents and approval layers can distinguish safe discovery from mutations.

Framework-specific integrations should adapt `ToolRegistry`; they should not introduce a second tool definition format.
