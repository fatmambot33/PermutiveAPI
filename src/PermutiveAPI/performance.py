"""Deterministic performance-budget measurement helpers."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Callable, Mapping, Sequence

PERFORMANCE_CONTRACT_VERSION = 1


@dataclass(frozen=True)
class PerformanceBudget:
    """Define one local performance regression budget."""

    name: str
    iterations: int
    max_median_seconds: float

    def __post_init__(self) -> None:
        """Validate positive budget values."""
        if not self.name:
            raise ValueError("Performance budget name must not be empty.")
        if self.iterations < 1:
            raise ValueError("Performance budget iterations must be positive.")
        if self.max_median_seconds <= 0:
            raise ValueError("Performance budget duration must be positive.")


@dataclass(frozen=True)
class PerformanceResult:
    """Contain deterministic summary evidence for one measured operation."""

    name: str
    iterations: int
    median_seconds: float
    max_median_seconds: float

    @property
    def ok(self) -> bool:
        """Return whether the measured median remains within its budget."""
        return self.median_seconds <= self.max_median_seconds

    def to_dict(self) -> dict[str, object]:
        """Return machine-readable result evidence."""
        return {
            "name": self.name,
            "iterations": self.iterations,
            "median_seconds": round(self.median_seconds, 9),
            "max_median_seconds": self.max_median_seconds,
            "ok": self.ok,
        }


def measure_operation(
    operation: Callable[[], object],
    budget: PerformanceBudget,
    *,
    clock: Callable[[], float] = time.perf_counter,
) -> PerformanceResult:
    """Measure an operation repeatedly and compare its median with a budget."""
    operation()
    durations = []
    for _ in range(budget.iterations):
        started = clock()
        operation()
        durations.append(max(0.0, clock() - started))
    return PerformanceResult(
        name=budget.name,
        iterations=budget.iterations,
        median_seconds=median(durations),
        max_median_seconds=budget.max_median_seconds,
    )


def load_performance_budgets(path: Path) -> tuple[PerformanceBudget, ...]:
    """Read and validate versioned performance budgets from JSON."""
    decoded: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError("Performance budget evidence must be a JSON object.")
    if decoded.get("version") != PERFORMANCE_CONTRACT_VERSION:
        raise ValueError("Unsupported performance budget version.")
    raw_budgets = decoded.get("budgets")
    if not isinstance(raw_budgets, list):
        raise TypeError("Performance budgets must be a list.")
    budgets = []
    for raw in raw_budgets:
        if not isinstance(raw, dict):
            raise TypeError("Every performance budget must be an object.")
        name = raw.get("name")
        iterations = raw.get("iterations")
        maximum = raw.get("max_median_seconds")
        if not isinstance(name, str):
            raise TypeError("Performance budget name must be a string.")
        if not isinstance(iterations, int) or isinstance(iterations, bool):
            raise TypeError("Performance budget iterations must be an integer.")
        if not isinstance(maximum, (int, float)) or isinstance(maximum, bool):
            raise TypeError("Performance budget duration must be numeric.")
        budgets.append(PerformanceBudget(name, iterations, float(maximum)))
    names = [budget.name for budget in budgets]
    if len(names) != len(set(names)):
        raise ValueError("Performance budget names must be unique.")
    return tuple(sorted(budgets, key=lambda item: item.name))


def performance_report(results: Sequence[PerformanceResult]) -> dict[str, object]:
    """Return versioned deterministic performance evidence."""
    ordered = tuple(sorted(results, key=lambda item: item.name))
    return {
        "version": PERFORMANCE_CONTRACT_VERSION,
        "ok": bool(ordered) and all(result.ok for result in ordered),
        "results": [result.to_dict() for result in ordered],
    }


def validate_operation_names(
    budgets: Sequence[PerformanceBudget],
    operations: Mapping[str, Callable[[], object]],
) -> None:
    """Require an exact one-to-one mapping between budgets and operations."""
    budget_names = {budget.name for budget in budgets}
    operation_names = set(operations)
    if budget_names != operation_names:
        missing = sorted(budget_names - operation_names)
        extra = sorted(operation_names - budget_names)
        raise ValueError(
            "Performance operations do not match budgets: "
            f"missing={missing}, extra={extra}."
        )


__all__ = [
    "PERFORMANCE_CONTRACT_VERSION",
    "PerformanceBudget",
    "PerformanceResult",
    "load_performance_budgets",
    "measure_operation",
    "performance_report",
    "validate_operation_names",
]
