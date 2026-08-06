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

## Completed — 6.6: platform proof

Tracking issue: #186

- [x] #196 — Deterministic policy, tool-selection, security, workflow, and audit scorecards.
- [x] #197 — Local mock Permutive server and reusable fixtures.
- [x] #198 — End-to-end governed agent scenarios.
- [x] #199 — Versioned capability negotiation across tools, plugin, AgentKit, and MCP.
- [x] #200 — Actionable errors, executable recipes, and first-success budget.

The 6.6 platform proves governed behavior from tool discovery through realistic local HTTP execution. Adaptive integrations negotiate capabilities before execution, failures provide secret-safe next actions, and a fresh installed interpreter reaches a useful result inside an enforced five-second budget.

## Now — 6.7: operational reliability

Tracking issue: #187

- [ ] #204 — Generated API coverage, schema-drift detection, sanitized recording, and deterministic replay.
- [ ] #205 — Coordinated rate limits, atomic credential rotation, and sync/async stress safety.
- [ ] #206 — Performance budgets and immutable release-artifact validation.

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
