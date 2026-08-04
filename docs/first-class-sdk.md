# First-class SDK architecture

PermutiveAPI 6.1 establishes a small compatibility-bound core while preserving
legacy resource classes.

## Canonical boundaries

- `PermutiveClient` owns HTTP configuration, explicit timeouts, authentication,
  dependency injection, and transport lifecycle.
- `ClientConfig` is immutable and safe to share.
- `JSONObject` and related aliases define public JSON boundaries without
  exposing unconstrained dictionaries.
- `Page[T]` and `iter_pages()` provide one lazy, bounded pagination mechanism.
- `BatchResult[I, T]` and `BatchItemResult[I, T]` preserve input identity and
  separate values from item-level failures.

## Resource migration

Existing class methods remain supported for compatibility. New resource work
should accept a `PermutiveClient` or be implemented by a small service object
that uses it. Credentials must not be read implicitly by low-level methods.

Canonical remote operation names are `get`, `list`, `create`, `update`, and
`delete`. Unsupported operations should not be added. Existing legacy names are
deprecated only through a documented minor-release warning cycle.

## Retry and idempotency

GET and DELETE are treated as safe by the canonical client. POST and PATCH are
never assumed idempotent. An endpoint-specific idempotency mechanism must exist
before write retries are enabled. All calls use explicit connect/read timeouts.

## Serialization

Domain models retain explicit `to_json()` methods where API field mapping is
meaningful. JSON output must contain only JSON-compatible values. `None`, empty
lists, and empty dictionaries are omitted by the established serializer; empty
strings are preserved because they can be meaningful API values.

## Testing layers

1. Unit tests cover types, serialization, pagination, batching, errors, and URL
   construction without network access.
2. Contract tests use deterministic response fixtures.
3. Live integration tests are opt-in and credential guarded.

The default test suite must never access the network.
