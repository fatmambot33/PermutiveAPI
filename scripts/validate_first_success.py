"""Validate committed first-success evidence and execute the budget gate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from PermutiveAPI.first_success import (
    first_success_contract,
    measure_first_success,
)

CONTRACT_PATH = Path("metrics/first-success-v1.json")


def validate_first_success(path: Path = CONTRACT_PATH) -> None:
    """Raise when metric evidence drifts or the measured budget is exceeded."""
    committed: Any = json.loads(path.read_text(encoding="utf-8"))
    expected = first_success_contract()
    if committed != expected:
        raise SystemExit(
            "First-success evidence is stale. Regenerate "
            f"{path.as_posix()} from first_success_contract()."
        )
    measurement = measure_first_success(
        budget_seconds=float(expected["budget_seconds"]),
    )
    print(json.dumps(measurement.to_dict(), indent=2, sort_keys=True))
    if not measurement.ok:
        raise SystemExit("First-success budget exceeded or the recipe failed.")


def main() -> int:
    """Validate evidence and run the first-success measurement."""
    validate_first_success()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
