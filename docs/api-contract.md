# PermutiveAPI public SDK contract

This document defines the supported public surface and compatibility policy for
PermutiveAPI. It is the baseline for changes tracked in issue #92.

## Supported runtime

The current package metadata supports Python 3.8 through Python 3.12. The
supported range must remain aligned across `pyproject.toml`, CI, documentation,
and published package classifiers.

PermutiveAPI currently provides a synchronous API built on `requests`. Async
APIs are not part of the stable contract. Any future async client must use an
explicitly separate public type and must not silently change synchronous method
behavior.

## Public import surface

The stable public API is the set of names exported from `PermutiveAPI.__all__`.
Callers should import these names from the package root rather than from
implementation modules.

### Resource and request models

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

### Exceptions

- `PermutiveAPIError`
- `PermutiveAuthenticationError`
- `PermutiveBadRequestError`
- `PermutiveRateLimitError`
- `PermutiveResourceNotFoundError`
- `PermutiveServerError`

All other modules, classes, functions, constants, and attributes are internal
unless explicitly documented as public.

## Stability levels

Every exposed API belongs to one of these categories:

- **Stable:** exported from `PermutiveAPI.__all__` and covered by compatibility
  guarantees.
- **Provisional:** explicitly documented as provisional. It may change between
  minor releases with release-note notice.
- **Deprecated:** remains available temporarily and emits a
  `DeprecationWarning` with a replacement and removal target.
- **Internal:** not exported from the package root. It may change without
  notice.

At present, the package-root exports listed above are stable. No provisional or
deprecated package-root exports are currently declared.

## Compatibility guarantees

Within a major version, PermutiveAPI aims to preserve:

- package-root import names;
- public constructor and method parameter names;
- documented keyword argument behavior;
- documented return types;
- the base exception relationship through `PermutiveAPIError`;
- serialization behavior documented for public models.

The following are not guaranteed unless documented:

- implementation-module import paths;
- private names beginning with `_`;
- exact exception message wording;
- dictionary key ordering;
- undocumented response fields passed through from Permutive;
- network timing or retry count before retry behavior is formally specified.

## Deprecation policy

A planned breaking change should follow this sequence:

1. Introduce the replacement without removing the existing API.
2. Emit `DeprecationWarning` from the deprecated entry point.
3. Document the migration in the changelog and release notes.
4. Keep the deprecated API for at least one minor release and normally for
   three months, whichever is longer.
5. Remove it only in a major release, except for urgent security fixes or APIs
   that never shipped in a stable release.

Warnings must name the deprecated API, identify the replacement, and state the
earliest removal version when known.

## Return and error behavior

Public methods must have one documented success type. They should not return
`None` merely because transport, decoding, authentication, or server processing
failed. Such failures must raise a subclass of `PermutiveAPIError`.

Validation errors raised before a request should clearly identify the invalid
field. Authentication data, API keys, authorization headers, and sensitive
payload values must never appear in exception messages or representations.

## Credentials and configuration

Credentials may be supplied explicitly or loaded through documented workspace
configuration. Public behavior must not depend on undocumented environment
variables or mutable module-level state.

Any future client abstraction should make authentication, workspace context,
timeouts, and retry policy explicit and testable.

## Documentation rules

README examples and public documentation must:

- import stable names from `PermutiveAPI`;
- use only supported Python syntax;
- show documented return and exception behavior;
- avoid internal module imports;
- identify optional dependencies when required.

## Change checklist

A pull request that changes public behavior must answer:

- Does it add, remove, or rename a package-root export?
- Does it change a public signature or return type?
- Does it change serialization or exception behavior?
- Is a deprecation path required?
- Are package metadata, tests, examples, and release notes aligned?
