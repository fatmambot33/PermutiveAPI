# Roadmap

PermutiveAPI is moving from a completed first-class SDK and AI-native foundation to a verifiably trustworthy operating platform.

## Product objective

Make PermutiveAPI the most trustworthy and easiest way for Python developers and governed AI agents to operate Permutive safely.

## Completed — 6.5.1: product truth

Tracking issue: #185

- [x] #190 — Local product validation, lifecycle commands, release consistency, and clean-install gates.
- [x] #191 — Canonical SDK, CLI, credential, and optional-extra onboarding documentation.
- [x] #192 — Complete supported-surface strict typing and artifact contracts.

The 6.5.1 patch release establishes verifiable product claims, truthful onboarding, and an explicit canonical typing boundary.

## Now — 6.6: platform proof

Tracking issue: #186

- [x] #196 — Deterministic policy, tool-selection, security, workflow, and audit scorecards.
- [ ] #197 — Local mock Permutive server and reusable fixtures.
- [ ] #198 — End-to-end governed agent scenarios.
- [ ] #199 — Versioned capability negotiation across tools, plugin, and MCP.
- [ ] #200 — Actionable errors, executable recipes, and first-success budget.

The scorecard now proves local governance. The active slice adds realistic HTTP behavior without requiring live credentials or external network access.

## Then — 6.7: operational reliability

Tracking issue: #187

- Detect upstream request and response schema drift.
- Add sanitized HTTP recording and deterministic replay.
- Coordinate rate limits across sync and async concurrency.
- Add credential rotation, stress tests, and performance budgets.
- Validate release candidates from immutable publishable artifacts.

## Non-goals until demonstrated demand

- Plugin UI or dashboard.
- Additional agent-framework adapters.
- Third-party plugin marketplace.
- Webhook or bulk-workflow frameworks.
- Response caching or circuit breakers.
- OpenTelemetry and additional dataframe engines.

## Product rules

1. Maintain one canonical SDK and tool contract.
2. Prefer explicit behavior over magic.
3. Prioritize reliability and safety over feature breadth.
4. Require tests, documentation, and machine-readable evidence for every capability.
5. Keep credentials local and never echo, commit, or remotely store them.
6. Keep mutations governed by the declared approval policy.
7. Keep `main` releasable after every merge.

The canonical roadmap index is issue #188.
