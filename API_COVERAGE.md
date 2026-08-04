# API Coverage

This matrix defines the Permutive API and integration surfaces supported by the canonical SDK.

| Domain | Canonical entry point | Operations | Status |
|---|---|---|---|
| Cohorts | `client.cohorts` | get, list, iterate, create, update, delete | Supported |
| Imports | `client.imports` | get, list, iterate, create, update, delete | Supported |
| Segments | `client.segments` | get, list, iterate, create, update, delete | Supported |
| Sources | `client.sources` | get, list, iterate, create, update, delete | Supported |
| Workspaces | `client.workspaces` | get, list, iterate, create, update, delete | Supported |
| Agent capabilities | `PermutiveAgentKit` | discover and expose SDK capabilities | Supported |
| Tool integration | `ToolRegistry` / `tool` | register, describe, and invoke typed tools | Supported |
| MCP composition | `PermutiveMCPConfig` | compose hosted MCP server configuration | Supported |
| Codex plugin | `permutiveapi.plugins` entry point | load `CodexPlugin` | Supported |
| Identity | legacy `Identity` / `Alias` | identify and batch identify | Compatibility |
| User segmentation | legacy `Segmentation` / `Event` | segment and batch segment | Compatibility |
| Context segmentation | legacy `ContextSegment` | segment page context | Compatibility |

## Coverage rules

- A supported operation must have a documented SDK entry point, typed signature, deterministic contract test, and structured errors where transport is involved.
- Supported integration surfaces must have stable package-root imports and package metadata where discovery is required.
- Compatibility operations remain supported but may delegate to canonical transport code while their final resource shape is designed.
- An endpoint or integration not listed here is intentionally unsupported until it receives an implementation, tests, and documentation.
- API-specific behavior belongs in resource or action adapters, not in generic transport code.

## Adding coverage

Every new endpoint change must update this matrix and include request, response, error, and pagination tests where applicable. Every new integration surface must update `PUBLIC_API.md`, package metadata, and import-level regression tests.
