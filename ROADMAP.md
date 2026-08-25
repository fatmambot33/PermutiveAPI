# Roadmap

PermutiveAPI is a typed, governed AI-native Python platform for the Permutive API. The 6.8 roadmap focuses on consolidation rather than speculative breadth.

## Product objective

Make PermutiveAPI the most trustworthy and easiest way for Python developers and governed AI agents to operate Permutive safely, with one canonical contract per capability.

## 6.8.0 — lean first-class SDK consolidation

Tracking issue: #218

- [x] #219 — Classify and protect the canonical package-root SDK surface.
- [x] #220 — Add a single typed registry for canonical resources and operations.
- [x] #221 — Keep all new consolidation modules in strict Pyright scope and protect downstream typing boundaries.
- [x] #222 — Add one typed registry for tool, agent, plugin, and MCP integration metadata.
- [x] #223 — Preserve one shared secret-safe resilience and diagnostic policy across the existing sync/async runtime.
- [x] #224 — Produce deterministic combined consolidation evidence and fingerprints.
- [x] #225 — Add regression tests for contract drift, duplicate operations, classification completeness, and reproducibility.
- [x] #226 — Reuse the existing protected CI/release gates for the exact 6.8 candidate rather than adding another workflow family.
- [x] #227 — Publish one concise 6.8 consolidation/release guide while preserving canonical workflow documentation.
- [x] #228 — Prepare the exact 6.8.0 release candidate; close only after public PyPI verification succeeds.

### 6.8 design rules

1. No second client, transport, resource facade, query DSL, plugin runtime, or governance model.
2. New public behavior must be strictly typed and backward compatible within 6.x.
3. Generated evidence must be deterministic and derived from executable contracts.
4. Integration metadata may be shared only where semantics are genuinely common.
5. Compatibility APIs remain available but are not allowed to define new canonical behavior.
6. Security, release, and public-PyPI verification gates remain mandatory.

## Completed foundations

### 6.5.1 — product truth

Established verifiable product claims, truthful onboarding, lifecycle validation, clean-install checks, and an explicit canonical typing boundary.

### 6.6 — platform proof

Proved governed behavior from tool discovery through realistic local HTTP execution, with capability negotiation, actionable failures, executable recipes, and a measured first-success budget.

### 6.7 — operational reliability

Added generated API coverage, structural response drift detection, sanitized recording/replay, shared rate-limit coordination, atomic credential rotation, performance budgets, and immutable release evidence.

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
8. Require a measured problem and acceptance criteria before extending the roadmap.
