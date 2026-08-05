# Changelog

All notable changes to PermutiveAPI are documented here. The project follows Semantic Versioning.

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
