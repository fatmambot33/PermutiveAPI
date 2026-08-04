# API Coverage

This matrix defines the Permutive API surface supported by the canonical SDK.

| Domain | Canonical entry point | Operations | Status |
|---|---|---|---|
| Cohorts | `client.cohorts` | get, list, iterate, create, update, delete | Supported |
| Imports | `client.imports` | get, list, iterate, create, update, delete | Supported |
| Segments | `client.segments` | get, list, iterate, create, update, delete | Supported |
| Sources | `client.sources` | get, list, iterate, create, update, delete | Supported |
| Workspaces | `client.workspaces` | get, list, iterate, create, update, delete | Supported |
| Identity | legacy `Identity` / `Alias` | identify and batch identify | Compatibility |
| User segmentation | legacy `Segmentation` / `Event` | segment and batch segment | Compatibility |
| Context segmentation | legacy `ContextSegment` | segment page context | Compatibility |

## Coverage rules

- A supported operation must have a documented SDK entry point, typed signature, deterministic contract test, and structured errors.
- Compatibility operations remain supported but may delegate to canonical transport code while their final resource shape is designed.
- An endpoint not listed here is intentionally unsupported until it receives an implementation, tests, and documentation.
- API-specific behavior belongs in resource or action adapters, not in generic transport code.

## Adding coverage

Every new endpoint change must update this matrix and include request, response, error, and pagination tests where applicable.
