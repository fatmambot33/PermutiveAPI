# Public API Contract

PermutiveAPI exposes one canonical SDK surface and a temporary compatibility surface. This document is the source of truth for public support, deprecation, and removal decisions.

## Canonical surface

New code should import these names from `PermutiveAPI`.

### Synchronous client and resources

- `PermutiveClient`
- `Resource`

### Asynchronous client and resources

- `AsyncPermutiveClient`
- `AsyncResource`
- `AsyncResponse`
- `AsyncTransport`
- `execute_async_batch`

### Typed request contracts

- `AliasPayload`
- `IdentityPayload`
- `EventPayload`
- `SegmentationPayload`
- `ContextPayload`

### Query composition

- `QueryExpression`
- `all_of`
- `any_of`
- `event`
- `in_segment`
- `property_condition`

### Configuration and secrets

- `PermutiveConfig`
- `Secret`

### Typed SDK primitives

- `JSONObject`
- `JSONScalar`
- `JSONValue`
- `Page`
- `RetryPolicy`
- `BatchItem`
- `BatchResult`
- `execute_batch`

### Agent and tool integration

- `PermutiveAgentKit`
- `JSONSchema`
- `ToolDefinition`
- `ToolHandler`
- `ToolRegistry`
- `tool`

### MCP integration

- `PermutiveMCPConfig`
- `PERMUTIVE_MCP_DOCUMENTATION_URL`
- `PERMUTIVE_MCP_SERVER_NAME`
- `PERMUTIVE_MCP_TOKEN_ENV`
- `PERMUTIVE_MCP_URL_ENV`

### Canonical errors

- `SDKError`
- `AuthenticationError`
- `AuthorizationError`
- `ConflictError`
- `DecodingError`
- `NotFoundError`
- `RateLimitError`
- `ServerError`
- `TransportError`
- `ValidationError`

The canonical surface follows semantic versioning and is implemented only by modules listed in the strict group of `TYPING_SCOPE.json`. Those modules must pass Pyright strict mode.

Backward-compatible additions may ship in minor releases. Breaking changes require a major release.

## Compatibility surface

These legacy exports remain supported for existing users while resource operations are migrated to the canonical client:

- `Alias`
- `Cohort`
- `CohortList`
- `ContextSegment`
- `Event`
- `Identity`
- `Import`
- `ImportList`
- `Segment`
- `SegmentList`
- `Segmentation`
- `Source`
- `Workspace`
- `WorkspaceList`
- `PermutiveAPIError`
- `PermutiveAuthenticationError`
- `PermutiveBadRequestError`
- `PermutiveRateLimitError`
- `PermutiveResourceNotFoundError`
- `PermutiveServerError`

Compatibility exports may be implemented as thin adapters, but must not define an independent transport contract. Their implementation modules are explicitly listed in the compatibility group of `TYPING_SCOPE.json`; exclusions cannot grow silently.

They will not be removed without a documented deprecation period and migration path.

## Deprecated surface

No package-root export is deprecated at this time.

When an export becomes deprecated, the change must include:

1. a runtime deprecation warning where practical;
2. migration guidance;
3. a changelog entry;
4. the earliest major version in which removal may occur.

## Internal surface

Anything not listed in `PermutiveAPI.__all__` is internal unless a public document explicitly states otherwise. Internal helpers may change without notice. Users must not rely on imports from private modules or names prefixed with `_`.

Documented secondary surfaces such as `PermutiveAPI.plugins.codex.CodexPlugin`, diagnostics wrappers, and governed AI-native execution types remain supported through their dedicated documentation and strict implementation modules without expanding the package-root namespace indefinitely.

## Decision rules

- Prefer `PermutiveClient` and `Resource` for new synchronous API work.
- Prefer `AsyncPermutiveClient` and `AsyncResource` for asynchronous work.
- Prefer typed query helpers over hand-built dictionaries when the schema is supported.
- Prefer `PermutiveAgentKit`, `ToolRegistry`, and `CodexPlugin` for agent integrations.
- Use `PermutiveMCPConfig` for MCP server composition rather than duplicating environment handling.
- Do not add a second way to perform the same operation without a documented product reason.
- Public methods require stable names, typed signatures, documented errors, tests, and examples.
- Compatibility code delegates to canonical code rather than duplicating request logic.
- New package-root exports require updates to this document and the public API contract tests.
- New package modules require an explicit strict or compatibility classification.

## Migration direction

Core CRUD resources are available through the canonical client. Identity, user segmentation, and context segmentation remain compatibility actions while their endpoint contracts stabilize. Agent, tool, plugin, and MCP integrations are canonical extension surfaces.

`API_COVERAGE.md` records exact endpoint support, `MIGRATION.md` provides supported transitions, and `docs/TYPING.md` explains the strict implementation boundary.
