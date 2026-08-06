"""Tests for executable recipes and first-success metrics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from PermutiveAPI.cli import examples
from PermutiveAPI.first_success import (
    first_success_contract,
    measure_first_success,
)
from PermutiveAPI.recipes import (
    RecipeCategory,
    find_recipes,
    recipe_catalog,
    recipe_manifest,
)


def _execute(source: str, name: str) -> object:
    namespace: dict[str, Any] = {"__name__": f"recipe_{name}"}
    exec(compile(source, name, "exec"), namespace)
    main = namespace.get("main")
    assert callable(main)
    return main()


def test_recipe_catalog_covers_every_supported_category() -> None:
    """Recipe discovery exposes the complete documented category set."""
    assert {recipe.category for recipe in recipe_catalog()} == set(RecipeCategory)
    assert find_recipes(category="sdk")
    assert find_recipes(category=RecipeCategory.GOVERNED)


def test_required_product_recipes_are_present() -> None:
    """The four roadmap workflows remain directly discoverable."""
    names = {recipe.name for recipe in recipe_catalog()}
    assert {
        "workspace-inspection",
        "cohort-analysis",
        "segment-comparison",
        "reviewed-cohort-write",
    } <= names


def test_every_recipe_compiles_and_runs_without_credentials() -> None:
    """All published recipes execute through canonical local surfaces."""
    for recipe in recipe_catalog():
        assert recipe.credential_free is True
        result = _execute(recipe.source, recipe.name)
        assert result is not None


def test_typed_query_recipe_uses_the_canonical_serialization_contract() -> None:
    """The query recipe returns the documented deterministic JSON object."""
    recipe = find_recipes(name="typed-query")[0]
    result = _execute(recipe.source, recipe.name)

    assert result == {
        "and": [
            {
                "event": "pageview",
                "frequency": {"greater_than_or_equal_to": 1},
            },
            {"in_segment": "high-intent"},
        ]
    }


def test_recipe_manifest_is_deterministic() -> None:
    """Machine-readable recipe metadata is stable and JSON serializable."""
    manifest = recipe_manifest()
    serialized = json.dumps(manifest, sort_keys=True)

    assert manifest["version"] == 1
    assert "workspace-inspection" in serialized
    assert serialized == json.dumps(recipe_manifest(), sort_keys=True)


def test_cli_lists_filters_prints_and_serializes_recipes(capsys: Any) -> None:
    """CLI users can discover categories or copy one complete recipe."""
    assert examples(category="queries") == 0
    listing = capsys.readouterr().out
    assert "typed-query" in listing

    assert examples(name="workspace-inspection") == 0
    source = capsys.readouterr().out
    assert "PermutiveClient" in source
    compile(source, "workspace-inspection", "exec")

    assert examples(category="plugin", as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["category"] == "plugin"


def test_first_success_contract_matches_committed_evidence() -> None:
    """The measured budget cannot drift from committed evidence."""
    committed = json.loads(
        Path("metrics/first-success-v1.json").read_text(encoding="utf-8")
    )
    assert committed == first_success_contract()


def test_first_success_completes_in_a_fresh_process() -> None:
    """An installed interpreter reaches a useful result inside the budget."""
    measurement = measure_first_success()

    assert measurement.ok is True
    assert measurement.duration_seconds <= measurement.budget_seconds
    assert measurement.error_type is None
