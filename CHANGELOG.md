# Changelog

All notable changes to PermutiveAPI are documented here. The project follows Semantic Versioning.

## 6.1.0 - 2026-08-04

### Added
- Explicit public SDK compatibility contract.
- Strict package typing configuration and PEP 561 packaging declaration.
- Python 3.13 validation.
- Clean-wheel installation validation in CI.

### Changed
- Raised the supported Python floor to 3.9.
- Made pandas an optional `dataframe` integration instead of a core dependency.
- Normalized the package version to PEP 440 and advanced the minor version from 6.0.x to 6.1.0.
- Updated GitHub Actions to current major versions.

### Existing first-class capabilities audited
- Shared reusable HTTP sessions and centralized request dispatch.
- Explicit request timeouts.
- Typed exception classes and secret redaction.
- Bounded retries and `Retry-After` support.
- Shared JSON serialization helpers.
- Typed batch request and progress models.
- Unit and integration-test separation.

## Release policy

- Patch releases contain backward-compatible fixes.
- Minor releases contain backward-compatible features and deprecations.
- Major releases may remove deprecated APIs or introduce breaking changes.
