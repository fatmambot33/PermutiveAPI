# Compatibility Matrix

This document defines the public compatibility contract for PermutiveAPI 6.4.

## Supported runtimes

| Area | Contract |
|---|---|
| Python | 3.9 through 3.13 |
| Core install | Imports without optional integrations |
| Public API | Symbols listed in `PermutiveAPI.__all__` |
| Canonical client | `PermutiveClient` |
| Canonical resources | `Resource[T]` namespaces |
| Compatibility exports | Supported until a documented deprecation cycle completes |

## Stable behavior

The following behavior is protected by regression tests:

- package-root exports remain explicit and importable;
- canonical resource methods remain `get`, `list`, `list_page`, `iter_all`, `create`, `update`, and `delete`;
- `PermutiveClient` continues to expose `cohorts`, `imports`, `segments`, `sources`, and `workspaces` resource namespaces;
- compatibility exports delegate to the canonical implementation rather than defining a second transport contract;
- the core package imports without pandas, Polars, OpenTelemetry, CLI, async, MCP, or plugin extras;
- backward-compatible additions may ship in minor releases;
- removals require a documented deprecation period and a major release.

## Change policy

| Change | Allowed release |
|---|---|
| Add optional public symbol | Minor |
| Add optional method parameter | Minor |
| Add response field support | Patch or minor |
| Change public signature incompatibly | Major |
| Remove compatibility export | Major, after deprecation |
| Require a new optional dependency in core | Not allowed |

## Validation

CI must fail when:

- a package-root export is added or removed without updating the public contract;
- a canonical resource method disappears or changes its required parameter shape;
- a supported resource namespace disappears;
- an optional integration becomes required for a core import;
- a compatibility export is removed without migration and deprecation records.
