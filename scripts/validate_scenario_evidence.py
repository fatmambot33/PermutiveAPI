"""Validate committed governed-scenario recipes and HTTP fixtures."""

from __future__ import annotations

import json
from pathlib import Path

from PermutiveAPI.scenario_fixtures import scenario_fixture_catalog
from PermutiveAPI.scenarios import scenario_recipe_catalog


def _load(path: Path) -> object:
    """Load one committed JSON evidence file."""
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    """Compare committed scenario evidence with runtime contracts."""
    checks = (
        (
            Path("scenarios/fixtures-v1.json"),
            scenario_fixture_catalog(),
        ),
        (
            Path("scenarios/recipes.json"),
            scenario_recipe_catalog(),
        ),
    )
    errors = []
    for path, expected in checks:
        if not path.is_file():
            errors.append(f"missing scenario evidence: {path}")
            continue
        if _load(path) != expected:
            errors.append(f"stale scenario evidence: {path}")
    if errors:
        print("Scenario evidence validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Scenario evidence validation passed for fixtures and recipes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
