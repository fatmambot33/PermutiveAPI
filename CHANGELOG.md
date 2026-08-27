# Changelog

All notable changes to PermutiveAPI are documented here. The project follows Semantic Versioning.

## 6.8.3 - 2026-08-27

### Changed
- Adopted the AI Native Platform v0.2 security-evidence contract and pinned the registered consumer to the revised MCP-optional v0.2 contract.
- Kept repository security evidence explicit while preserving the existing MCP surface.
- Simplified solo-maintainer CI protection around the aggregate `ci`, AI-native `validate`, and CodeQL gates without reducing underlying lint, Python-version, package, or security coverage.

### Compatibility
- No public Python SDK, transport, resource, query, plugin, MCP, or governance API is removed or changed incompatibly.
- This patch synchronizes the published source distribution with the current AI-native contract and repository validation state.

## 6.8.2 - 2026-08-26

### Fixed
- Constrained the optional dataframe integration and development dependency to `pandas>=2,<3`.
- Kept pandas 3 outside the supported dependency range until compatibility is explicitly validated.

### Compatibility
- Core installs remain unchanged because pandas is still optional.
- Existing pandas 2.x users remain supported without requiring a pandas 3 upgrade.

## 6.8.1 - 2026-08-25

### Fixed
- Published the five post-merge 6.8 review fixes that landed on `main` after the immutable 6.8.0 artifact had already been built.
- Made canonical sync resource facades derive their transport paths from the typed resource registry instead of duplicating endpoint strings.
- Corrected integration governance metadata so plain tool discovery is not mislabeled as governed invocation.
- Strengthened public-surface classification and consolidation tests to prevent export drift and incomplete compatibility classification.
- Extended clean-wheel CI validation to import the 6.8 consolidation modules from the built distribution.

### Release integrity
- 6.8.1 is the canonical corrected 6.8 patch and is built from the post-review `main` state.
- 6.8.0 remains immutable and valid as published, but does not contain the post-merge review fixes above.

## 6.8.0 - 2026-08-25

### Added
- A typed canonical resource registry covering the five supported resource families and 25 CRUD/list operations.
- Deterministic package-root public-surface classification for canonical, integration, and compatibility exports.
- A shared typed integration registry for tools, AgentKit, the Codex plugin, and MCP composition.
- Reproducible consolidation evidence combining public API, resource, integration, and capability contracts with a stable SHA-256 fingerprint.
- Regression tests for resource-contract drift, duplicate operations, public-export classification completeness, governed integration metadata, and evidence reproducibility.

### Changed
- Added all 6.8 consolidation modules to the strict Pyright and typing-scope contracts.
- Consolidated the roadmap around one canonical contract per capability rather than adding speculative integrations.
- Reused the existing protected CI, security, artifact, Trusted Publishing, and public-PyPI verification gates for the exact 6.8 candidate.

### Compatibility
- No supported 6.x package-root import is removed.
- Legacy resources remain explicit compatibility APIs while canonical sync, async, query, plugin, MCP, and governance behavior remains unchanged.

## 6.7.1 - 2026-08-24

### Fixed
- Republished the completed 6.7 release line as a new immutable patch version after the public PyPI index no longer exposed 6.7.0.
- Kept the 6.7.0 product, API, security, typing, and operational-reliability behavior unchanged.

## 6.7.0 - 2026-08-06

### Added
- Deterministic governed-platform evaluation types and a versioned machine-readable scorecard.
- `permutiveapi eval` for credential-free, network-free policy and workflow verification.
- A dependency-free loopback mock Permutive server with versioned success, error, retry, and pagination fixtures.
- Deterministic governed end-to-end scenarios over the canonical SDK, policy engine, workflow runner, and audit sink.
- Versioned capability discovery and negotiation across `ToolRegistry`, `PermutiveAgentKit`, the Codex plugin, and MCP configuration.
- Stable capability negotiation error codes, committed contract evidence, and cross-surface compatibility tests.
- Secret-safe `ErrorGuidance` with stable codes, retryability, recommended actions, and sanitized context.
- `CodexPlugin.invoke_safe()` for actionable structured plugin results without changing existing invocation behavior.
- Seven executable credential-free recipes across SDK, async, queries, plugin, and governed workflow categories.
- CLI recipe discovery, filtering, source printing, and machine-readable JSON output.
- A committed installation-to-first-success metric with a five-second fresh-process budget.
- Versioned endpoint contracts, generated API coverage, and additive-versus-breaking response drift classification.
- Secret-safe HTTP recording and deterministic replay through the canonical client.
- A manual, protected, read-only live integration workflow with no committed credential or endpoint.
- Shared synchronous and asynchronous rate-limit coordination with `Retry-After` propagation.
- Atomic credential generations and transport wrappers for safe rotation without mutating in-flight attempts.
- Deterministic performance budgets for contract generation, query serialization, recipes, and recordings.
- Immutable release manifests containing source commit, artifact size, and SHA-256 evidence.
- CI enforcement for evaluations, scenarios, capabilities, recipes, first success, contracts, replay, performance, strict typing, and clean-wheel execution.

### Security
- Governed tool failures retain the exception type but no longer expose raw exception messages.
- Evaluation runner failures report only the exception type and never persist secret-bearing text.
- Scenario and capability metadata exclude credentials, bearer tokens, custom MCP header values, and request payloads.
- Actionable errors exclude credentials, payloads, raw messages, and URL query strings.
- Recordings exclude request payloads, query parameters, authorization headers, cookies, and recursively sensitive response values.
- Atomic credential snapshots are redacted in representations and never written to evidence.
- Unsupported intents, denied writes, missing approvals, and incompatible capability requirements fail before HTTP or tool execution.
- Live integration remains opt-in, read-only, and environment-gated.

### Changed
- Declared AI evaluations as an evidence-backed platform capability.
- Added evaluation, mock-server, scenario, capability, actionable-error, recipe, first-success, contract, recording, resilience, performance, and release-evidence modules to strict typing and package contracts.
- Preserved the historical Codex plugin API value `1` while exposing the normalized negotiation version `1.0`.
- Made `permutiveapi examples` an executable installed-product surface rather than a static path listing.
- Generated `API_COVERAGE.md` from maintained samples and canonical endpoint metadata.
- Made PyPI and GitHub Release consume the exact artifact set verified after build.
- Enabled the same protected validation jobs for agent-authored branches.

## 6.5.1 - 2026-08-06

### Added
- Deterministic `permutiveapi validate`, `test`, `docs`, `examples`, `upgrade`, and `uninstall` lifecycle commands.
- Secret-free installed-product checks for package metadata, Python plugin discovery, and the public tool contract.
- AI-native evidence validation for declared commands and conditional evaluation or benchmark claims.
- Release metadata consistency checks for package version, changelog headings, release notes, and release tags.
- Clean-install validation for the core SDK, CLI, Python plugin, async extra, and dataframe extra.
- Versioned `TYPING_SCOPE.json` with strict and compatibility module classifications.
- Downstream strict type-consumption examples and complete canonical wheel-content checks.

### Changed
- Published the active 6.5.1–6.7 roadmap with explicit product rules and non-goals.
- Replaced legacy onboarding with one canonical guide for the sync SDK, async extra, dataframe extra, CLI, Codex plugin, and project contracts.
- Simplified local credential setup to require only `PERMUTIVE_API_KEY`, the credential consumed by the SDK and plugin.
- Reconciled `PUBLIC_API.md` with the complete 6.5 package-root surface.
- Made strict Pyright coverage exactly match the declared canonical module group.
- Explicitly bounded legacy compatibility exclusions so new modules cannot silently escape typing review.

### Fixed
- Removed an unused diagnostics type import exposed by complete strict analysis.
- Corrected typed query examples to pass an iterable of expressions to `all_of`.
- Prevented machine-readable platform declarations from claiming unevidenced evaluations or benchmarks.

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
