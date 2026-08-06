"""Measure deterministic local operations against committed budgets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, cast

from PermutiveAPI.contracts import contract_manifest
from PermutiveAPI.performance import (
    load_performance_budgets,
    measure_operation,
    performance_report,
    validate_operation_names,
)
from PermutiveAPI.query_dsl import all_of, event, in_segment
from PermutiveAPI.recipes import recipe_manifest
from PermutiveAPI.recording import Recording
from PermutiveAPI.sdk import JSONValue

BUDGETS_PATH = Path("benchmarks/budgets-v1.json")
SAMPLES_PATH = Path("contracts/api-samples-v1.json")
RECORDING_PATH = Path("recordings/core-v1.json")


def main() -> int:
    """Run local regression measurements and fail when a budget is exceeded."""
    decoded: object = json.loads(SAMPLES_PATH.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict) or not isinstance(decoded.get("samples"), dict):
        raise TypeError("API samples are invalid.")
    samples = cast(dict[str, JSONValue], decoded["samples"])

    operations: dict[str, Callable[[], object]] = {
        "contract-manifest": lambda: contract_manifest(samples),
        "query-serialization": lambda: all_of(
            (event("pageview"), in_segment("high-intent"))
        ).to_json(),
        "recipe-discovery": recipe_manifest,
        "recording-load": lambda: Recording.read(RECORDING_PATH),
    }
    budgets = load_performance_budgets(BUDGETS_PATH)
    validate_operation_names(budgets, operations)
    results = [
        measure_operation(operations[budget.name], budget)
        for budget in budgets
    ]
    report = performance_report(results)
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["ok"]:
        raise SystemExit("One or more performance budgets were exceeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
