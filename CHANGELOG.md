# Changelog

All notable changes to PermutiveAPI are documented here. The project follows Semantic Versioning.

## 6.2.0 - 2026-08-04

### Added
- Explicit synchronous `PermutiveClient` with dependency-injected transport and connect/read timeouts.
- Stable typed JSON aliases and structured SDK exception hierarchy.
- Bounded retry policy with safe-method defaults, `Retry-After`, jitter, and attempt metadata.
- Generic typed `Page[T]`, lazy iteration, maximum-item bounds, and repeated-token protection.
- Ordered bounded `BatchResult` execution with per-item errors, fail-fast mode, and progress callbacks.
- Canonical typed `Resource[T]` CRUD/list facade.
- Deterministic contract tests for transport, redaction, retry safety, pagination, and batch behavior.

### Changed
- Exported the new stable client primitives at package root while retaining legacy exports.
- Enforced strict static analysis on the stable SDK surface.
- Kept pandas behind the optional `dataframe` integration.
- Enforced a 70% branch-aware coverage floor.
- Advanced the minor version from 6.1.0 to 6.2.0.

## 6.1.0 - 2026-08-04

### Added
- Explicit public SDK compatibility contract.
- PEP 561 packaging declaration and Python 3.13 validation.
- Clean-wheel installation validation in CI.

### Changed
- Raised the supported Python floor to 3.9.
- Made pandas an optional `dataframe` integration instead of a core dependency.
- Normalized the package version to PEP 440 and advanced one minor version from 6.0.x to 6.1.0.
- Updated GitHub Actions to current major versions.

## Release policy

- Patch releases contain backward-compatible fixes.
- Minor releases contain backward-compatible features and deprecations.
- Major releases may remove deprecated APIs or introduce breaking changes.
