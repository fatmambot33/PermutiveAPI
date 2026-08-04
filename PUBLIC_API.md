# Public API Contract

PermutiveAPI exposes one canonical SDK surface and a temporary compatibility surface.
This document is the source of truth for public support, deprecation, and removal decisions.

## Canonical surface

New code should use these exports from `PermutiveAPI`:

### Client and resources

- `PermutiveClient`
- `Resource`

### Typed request contracts

- `AliasPayload`
- `IdentityPayload`
- `EventPayload`
- `SegmentationPayload`
- `ContextPayload`

### Typed SDK primitives

- `JSONObject`
- `JSONScalar`
- `JSONValue`
- `Page`
- `RetryPolicy`
- `BatchItem`
- `BatchResult`
- `execute_batch`

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

The canonical surface is covered by compatibility tests and follows semantic versioning.
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

Compatibility exports may be implemented as thin adapters, but must not define an independent transport contract.
They will not be removed without a documented deprecation period and migration path.

## Deprecated surface

No package-root export is deprecated at this time.
When an export becomes deprecated, the change must include:

1. a runtime deprecation warning where practical;
2. migration guidance;
3. a changelog entry;
4. the earliest major version in which removal may occur.

## Internal surface

Anything not listed in `PermutiveAPI.__all__` is internal unless a public document explicitly states otherwise.
Internal modules, helpers, and implementation details may change without notice.
Users must not rely on imports from private modules or names prefixed with `_`.

## Decision rules

- Prefer `PermutiveClient` and `Resource` for all new work.
- Do not add a second way to perform the same operation without a documented product reason.
- Public methods require stable names, typed signatures, documented errors, tests, and examples.
- Compatibility code delegates to canonical code rather than duplicating request logic.
- New package-root exports require an update to this document and the public API contract test.

## Migration direction

Core CRUD resources are available through the canonical client. Identity, user segmentation, and context segmentation remain compatibility actions while their endpoint contracts stabilize. `API_COVERAGE.md` records the exact support status and `MIGRATION.md` provides the supported transition path.
