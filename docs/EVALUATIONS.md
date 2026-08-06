# AI evaluation contract

PermutiveAPI evaluates its governed agent surface with deterministic, network-free cases. The scorecard verifies platform behavior directly; it does not call a model provider and does not require Permutive credentials.

## Run the scorecard

```bash
permutiveapi eval
```

The command prints versioned JSON and exits with `0` only when every case passes. The same result is committed at `evals/scorecard.json` and checked in CI:

```bash
python scripts/generate_evaluation_scorecard.py --check evals/scorecard.json
```

To regenerate the evidence intentionally:

```bash
python scripts/generate_evaluation_scorecard.py --output evals/scorecard.json
```

## Covered guarantees

The canonical scorecard verifies:

- deterministic tool selection for a unique capability;
- rejection of unsupported capabilities before execution;
- read-only execution without write approval;
- explicit approval for mutating tools;
- allow-list and deny-list enforcement;
- exception-message redaction in structured failures;
- idempotent write replay without duplicate mutation;
- bounded workflow step counts;
- explicit partial-failure behavior;
- complete structured audit events.

## Evidence format

The scorecard schema version is `1`. Each result contains a stable name, category, pass state, and safe detail. It deliberately excludes timestamps, credentials, payload values, and raw exception messages so repeated runs produce identical JSON.

## Adding a case

A new safety or capability guarantee requires:

1. a deterministic `EvaluationCase` in `PermutiveAPI.evaluations`;
2. positive and negative observations where applicable;
3. tests for behavior and redaction;
4. regeneration of `evals/scorecard.json`;
5. documentation of the new guarantee.

The evaluation harness catches unexpected runner errors and reports only the exception type. Evaluation infrastructure must never turn secret-bearing exception text into evidence.

## Scope

These evaluations prove the local governed execution contract. Live API integration and model-provider behavior remain separate, opt-in test layers. The local mock server and end-to-end scenarios are tracked by roadmap issues #197 and #198.
