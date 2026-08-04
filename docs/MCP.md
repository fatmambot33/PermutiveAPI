# Official Permutive MCP integration

PermutiveAPI supports the official hosted Permutive MCP server through a small,
typed configuration layer. The SDK does **not** proxy or reimplement MCP tools:
Permutive owns the server, authentication model, tool catalogue, and rollout.

Official documentation: <https://docs.permutive.com/api/mcp/introduction>

## Why this boundary exists

Use PermutiveAPI for deterministic Python applications, typed REST resources,
write operations, batching, and automation. Use the hosted MCP server for
agent-led audience discovery, campaign exploration, and audience measurement.
Keeping these boundaries separate prevents the Python package from duplicating
an evolving hosted interface or adding an MCP runtime to every installation.

## Configuration

Ask Permutive for access, the hosted MCP endpoint, and the authentication method
for your organization. Store supplied credentials outside source control:

```shell
export PERMUTIVE_MCP_URL="https://your-permutive-mcp-endpoint/mcp"
export PERMUTIVE_MCP_TOKEN="your-token"
```

Create and validate the configuration:

```python
from PermutiveAPI import PermutiveMCPConfig

config = PermutiveMCPConfig.from_env()
print(config.to_json())
```

The generated JSON follows the common HTTP MCP client shape:

```json
{
  "mcpServers": {
    "permutive": {
      "headers": {
        "Authorization": "Bearer your-token"
      },
      "type": "http",
      "url": "https://your-permutive-mcp-endpoint/mcp"
    }
  }
}
```

Copy the server entry into the configuration used by your MCP-capable client.
Client-specific configuration locations and supported transport names vary, so
consult the client documentation before installation.

## Explicit configuration

```python
from PermutiveAPI import PermutiveMCPConfig

config = PermutiveMCPConfig(
    url="https://your-permutive-mcp-endpoint/mcp",
    token="your-token",
    headers={"X-Organization": "example"},
)
```

The token is excluded from the object's representation to reduce accidental
credential leakage in logs. URLs must be absolute HTTPS URLs, credentials in
URLs are rejected, and conflicting authorization sources fail early.

## Stability contract

`PermutiveMCPConfig` provides a stable SDK-owned configuration API. Individual
MCP tools are discovered from Permutive's hosted server at runtime and are not
mirrored as Python methods. Tool additions or changes therefore do not require
a PermutiveAPI release unless the connection contract itself changes.
