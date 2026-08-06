# Roadmap

PermutiveAPI is moving from a completed first-class SDK and AI-native foundation to a verifiably trustworthy operating platform.

## Product objective

Make PermutiveAPI the most trustworthy and easiest way for Python developers and governed AI agents to operate Permutive safely.

## Now — 6.5.1: product truth

Tracking issue: #185

- Align package, changelog, tag, and release metadata.
- Require automated evidence for every declared AI-native capability.
- Reconcile declared lifecycle commands with the actual CLI.
- Add one local `permutiveapi validate` product-health command.
- Expand strict typing and clean-install validation across supported surfaces.
- Keep the release backward compatible and dependency-light.

## Next — 6.6: platform proof

Tracking issue: #186

- Add deterministic agent and policy evaluations.
- Add a local mock Permutive server and end-to-end workflow tests.
- Publish machine-readable evaluation results.
- Version tool, plugin, and MCP capability negotiation.
- Improve actionable errors and executable examples.

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
