# Changelog

All notable changes to PermutiveAPI are documented here. The project follows Semantic Versioning.

## Unreleased

### Changed
- Published the active 6.5.1–6.7 roadmap with explicit product rules and non-goals.
- Added deterministic installed-product, AI-native evidence, release metadata, and clean-install validation gates.
- Replaced legacy onboarding with one canonical guide for the sync SDK, async extra, dataframe extra, CLI, Codex plugin, and project contracts.
- Simplified local credential setup to require only `PERMUTIVE_API_KEY`, the credential consumed by the SDK and plugin.
- Explicitly labelled legacy resource classes as compatibility surfaces rather than the recommended entry point.

## 6.5.0 - 2026-08-05

### Added
- First-class Codex plugin surface with deterministic discovery and explicit read-only or read-write policy.
- Native Codex marketplace packaging and installation metadata.
- Local-only `permutiveapi configure` and `permutiveapi doctor` credential workflows.
- Governed tool execution with allow and deny policies and explicit approvals for mutating tools.
- Structured invocation results and normalized failures.
- Deterministic idempotency support for safe retries.
- Bounded multi-step workflow execution and audit-sink integration.
- Machine-readable AI-native platform manifest and dedicated CI validation.
- Issue-driven self-improvement workflow with governed autonomous discovery and PR preparation.
- Unified AgentKit access across local tools, workflows, and hosted MCP configuration.

### Security
- Credentials remain local, are never echoed, and must be ignored by Git.
- Mutating tools require the declared write mode and explicit confirmation.
- Breaking, security, credential, public API, permission, and release changes require human approval.

### Changed
- Repositioned the package as a typed, governed AI-native Python platform while preserving the canonical SDK.
- Adopted the canonical AI-native platform standard at verified revision `67ec7caf19ead3282ea1aad29c43906f59e64d67`.

## 6.4.1 - 2026-08-05

### Added
- Opt-in synchronous and asynchronous diagnostic transport wrappers.
- Framework-neutral structured request lifecycle events with method, safe endpoint, duration, status, request ID, retry attempt, and exception type metadata.
- Deterministic diagnostics contract tests and a standard-library logging adapter example.

### Security
- Diagnostic endpoints exclude query strings.
- Credentials, payloads, and exception messages are never emitted by the diagnostic contract.
- Diagnostics remain disabled by default and add no runtime dependency.

## 6.4.0 - 2026-08-05

### Added
- Strictly typed `AsyncPermutiveClient` with optional HTTPX transport and async context management.
- Typed async resource CRUD, pagination, lazy iteration, ordered bounded batches, partial-failure outcomes, and cancellation-safe cleanup.
- Immutable typed query composition helpers with deterministic JSON serialization.
- Shared typed configuration and redacted secret contracts with HTTPS-by-default endpoint validation.
- Public API, architecture, deprecation, packaging, and Python 3.9–3.13 compatibility contracts.

### Changed
- Preserved the synchronous API and legacy package-root exports while extending the canonical SDK surface.
- Kept async support optional through `PermutiveAPI[async]`.
- Strengthened strict typing, deterministic tests, wheel-content checks, clean-install validation, and release documentation.

### Scope
- The release intentionally ships the coherent async, query, configuration, and security foundation.
- CLI, telemetry, caching, webhooks, bulk workflows, additional dataframe integrations, mock-server tooling, schema monitoring, and advanced resilience features are deferred to later releases rather than expanding the 6.4 minor release indefinitely.

## 6.1.0 - 2026-08-04

### Added
- Explicit public SDK compatibility contract.
- Explicit synchronous `PermutiveClient` with dependency-injected transport and connect/read timeouts.
- Stable typed JSON aliases and structured SDK exception hierarchy.
- Bounded retry policy with safe-method defaults, `Retry-After`, jitter, and attempt metadata.
- Generic typed `Page[T]`, lazy iteration, maximum-item bounds, and repeated-token protection.
- Ordered bounded `BatchResult` execution with per-item errors, fail-fast mode, and progress callbacks.
- Canonical typed `Resource[T]` CRUD/list facade.
- Deterministic contract tests for transport, redaction, retry safety, pagination, and batch behavior.
- PEP 561 packaging declaration and Python 3.13 validation.
- Clean-wheel installation validation in CI.

### Changed
- Exported the new stable client primitives at package root while retaining legacy exports.
- Enforced strict static analysis on the stable SDK surface.
- Raised the supported Python floor to 3.9.
- Made pandas an optional `dataframe` integration instead of a core dependency.
- Enforced a 70% branch-aware coverage floor.
- Normalized the package version to PEP 440 and advanced one minor version from 6.0.x to 6.1.0.
- Updated GitHub Actions to current major versions.

## Release policy

- Patch releases contain backward-compatible fixes.
- Minor releases contain backward-compatible features and deprecations.
- Major releases may remove deprecated APIs or introduce breaking changes.
