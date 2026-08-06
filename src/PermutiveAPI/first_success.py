"""Measure installed-package time to a first successful canonical result."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass

FIRST_SUCCESS_CONTRACT_VERSION = 1
FIRST_SUCCESS_BUDGET_SECONDS = 5.0
FIRST_SUCCESS_RECIPE = "workspace-inspection"


@dataclass(frozen=True)
class FirstSuccessMeasurement:
    """Record one fresh-process first-success measurement."""

    recipe: str
    duration_seconds: float
    budget_seconds: float
    ok: bool
    error_type: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable measurement data."""
        return {
            "recipe": self.recipe,
            "duration_seconds": round(self.duration_seconds, 6),
            "budget_seconds": self.budget_seconds,
            "ok": self.ok,
            "error_type": self.error_type,
        }


def first_success_contract() -> dict[str, object]:
    """Return the stable metric definition and enforced ceiling."""
    return {
        "version": FIRST_SUCCESS_CONTRACT_VERSION,
        "metric": "installed_interpreter_start_to_first_success",
        "recipe": FIRST_SUCCESS_RECIPE,
        "budget_seconds": FIRST_SUCCESS_BUDGET_SECONDS,
        "network_required": False,
        "credentials_required": False,
    }


def measure_first_success(
    *,
    budget_seconds: float = FIRST_SUCCESS_BUDGET_SECONDS,
) -> FirstSuccessMeasurement:
    """Run a canonical recipe in a fresh interpreter and enforce the budget."""
    if budget_seconds <= 0:
        raise ValueError("budget_seconds must be positive.")
    program = f"""from PermutiveAPI.recipes import find_recipes
recipe = find_recipes(name={FIRST_SUCCESS_RECIPE!r})[0]
namespace = {{"__name__": "permutiveapi_first_success_recipe"}}
exec(compile(recipe.source, recipe.name, "exec"), namespace)
result = namespace["main"]()
assert isinstance(result, dict)
assert result.get("id") == "workspace-demo"
"""
    started = time.perf_counter()
    error_type: str | None = None
    try:
        completed = subprocess.run(
            [sys.executable, "-c", program],
            check=False,
            capture_output=True,
            text=True,
            timeout=budget_seconds,
        )
        ok = completed.returncode == 0
        if not ok:
            error_type = "ChildProcessError"
    except subprocess.TimeoutExpired:
        ok = False
        error_type = "TimeoutExpired"
    duration = time.perf_counter() - started
    return FirstSuccessMeasurement(
        recipe=FIRST_SUCCESS_RECIPE,
        duration_seconds=duration,
        budget_seconds=budget_seconds,
        ok=ok and duration <= budget_seconds,
        error_type=error_type,
    )


def main() -> int:
    """Measure first success and print JSON suitable for CI evidence."""
    measurement = measure_first_success()
    print(json.dumps(measurement.to_dict(), indent=2, sort_keys=True))
    return 0 if measurement.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "FIRST_SUCCESS_BUDGET_SECONDS",
    "FIRST_SUCCESS_CONTRACT_VERSION",
    "FIRST_SUCCESS_RECIPE",
    "FirstSuccessMeasurement",
    "first_success_contract",
    "measure_first_success",
]
