# Public API Contract

PermutiveAPI exposes one canonical SDK and governed platform surface plus a temporary compatibility surface. `PermutiveAPI.__all__`, the explicit public API tests, and this document define the support contract.

## Canonical surface

New code should import canonical names from `PermutiveAPI`.

### Clients and resources

- `PermutiveClient`, `Resource`
- `AsyncPermutiveClient`, `AsyncResource`, `AsyncResponse`, `AsyncTransport`
- `execute_batch`, `execute_async_batch`
- `Page`, `BatchItem`, `BatchResult`, `RetryPolicy`

### Typed data and queries

- `JSONObject`, `JSONScalar`, `JSONValue`, `JSONSchema`
- `AliasPayload`, `IdentityPayload`, `EventPayload`, `SegmentationPayload`, `ContextPayload`
- `QueryExpression`, `all_of`, `any_of`, `event`, `in_segment`, `property_condition`

### Configuration and errors

- `PermutiveConfig`, `Secret`
- `SDKError` and its authentication, authorization, validation, not-found, conflict, rate-limit, server, transport, and decoding subclasses
- `ErrorGuidance`, `classify_exception`

### Governed AI and integration

- `PermutiveAgentKit`
- `ToolDefinition`, `ToolHandler`, `ToolRegistry`, `tool`
- capability descriptor, requirement, negotiation, error, version, and manifest exports
- `PermutiveMCPConfig` and MCP constants
- installed recipe and first-success exports

Capability negotiation is deterministic and secret-free. Mutating execution remains governed by declared policy and explicit approval.

### API contracts and drift

- `API_CONTRACT_VERSION`
- `EndpointContract`, `ResponseKind`
- `DriftKind`, `SchemaDrift`, `SchemaDriftError`
- `endpoint_contract`, `endpoint_contracts`, `contract_manifest`
- `structural_schema`, `schema_fingerprint`
- `classify_response_schema`, `validate_response_schema`

Additive response fields and variants remain compatible. Removed fields, removed variants, and type changes are breaking.

### Recording and replay

- `RECORDING_FORMAT_VERSION`
- `RecordedInteraction`, `Recording`, `RecordingTransport`
- `ReplayTransport`, `ReplayMismatchError`, `sanitize_json`

Recordings never include request payloads, query parameters, credentials, authorization headers, or cookies.

### Operational resilience

- `AtomicCredentials`, `CredentialSnapshot`
- `RateLimitCoordinator`
- `CoordinatedTransport`, `CoordinatedAsyncTransport`
- `SyncTransport`, `AsyncTransportLike`, `AsyncResponseLike`

One credential store and coordinator may be shared across sync and async callers. Every attempt receives one immutable credential generation. `Retry-After` deferrals apply to every caller sharing the coordinator.

### Performance and release evidence

- `PERFORMANCE_CONTRACT_VERSION`
- `PerformanceBudget`, `PerformanceResult`
- `load_performance_budgets`, `measure_operation`, `performance_report`, `validate_operation_names`
- `RELEASE_EVIDENCE_VERSION`
- `ArtifactDigest`, `digest_file`, `create_release_manifest`, `write_release_manifest`, `verify_release_manifest`

Release evidence is deterministic and verifies artifact path, size, SHA-256, package version, project, and source commit.

## Compatibility surface

These legacy exports remain supported while their implementations delegate toward canonical transport and resource code:

- `Alias`
- `Cohort`, `CohortList`
- `ContextSegment`
- `Event`
- `Identity`
- `Import`, `ImportList`
- `Segment`, `SegmentList`
- `Segmentation`
- `Source`
- `Workspace`, `WorkspaceList`
- `PermutiveAPIError`
- `PermutiveAuthenticationError`
- `PermutiveBadRequestError`
- `PermutiveRateLimitError`
- `PermutiveResourceNotFoundError`
- `PermutiveServerError`

Compatibility exports will not be removed without a documented deprecation period and migration path. Their modules are explicitly bounded in `TYPING_SCOPE.json` and cannot grow silently.

## Stability rules

- Canonical APIs follow Semantic Versioning.
- Backward-compatible additions may ship in minor releases.
- Breaking changes require a major release and migration guidance.
- New package-root exports require explicit inventory and classification updates.
- New modules require strict or compatibility typing classification.
- Public operations require typed signatures, documented errors, deterministic tests, and examples.
- Evidence-bearing capabilities require a machine-readable validator.
- Compatibility code delegates to canonical code rather than creating a second transport contract.

## Internal surface

Anything not listed in `PermutiveAPI.__all__` is internal unless dedicated documentation explicitly declares a secondary surface. Internal helpers and names prefixed with `_` may change without notice.

`API_COVERAGE.md` records generated endpoint support. `docs/OPERATIONAL_RELIABILITY.md` describes drift, replay, coordinated limits, rotation, budgets, live testing, and immutable release evidence. `docs/TYPING.md` explains the strict implementation boundary.
