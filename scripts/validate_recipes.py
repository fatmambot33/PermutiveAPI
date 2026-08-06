"""Compile and execute the canonical PermutiveAPI recipe catalog."""

from __future__ import annotations

import importlib.util
from typing import Any

from PermutiveAPI.recipes import RecipeCategory, recipe_catalog


def validate_recipes() -> tuple[int, int]:
    """Compile every recipe and execute recipes with installed dependencies."""
    executed = 0
    skipped = 0
    for recipe in recipe_catalog():
        print(f"Validating recipe: {recipe.name}")
        code = compile(recipe.source, recipe.name, "exec")
        if (
            recipe.category is RecipeCategory.ASYNC
            and importlib.util.find_spec("httpx") is None
        ):
            print(f"Skipping optional recipe without httpx: {recipe.name}")
            skipped += 1
            continue
        namespace: dict[str, Any] = {"__name__": f"recipe_{recipe.name}"}
        try:
            exec(code, namespace)
            main = namespace.get("main")
            if not callable(main):
                raise TypeError("recipe has no callable main()")
            result = main()
            if result is None:
                raise ValueError("recipe returned no result")
        except Exception as exc:  # noqa: BLE001 - validation boundary adds context
            raise SystemExit(
                f"Recipe validation failed for {recipe.name}: {type(exc).__name__}"
            ) from exc
        executed += 1
    return executed, skipped


def main() -> int:
    """Validate recipe compilation and execution."""
    executed, skipped = validate_recipes()
    print(f"Recipe validation passed: {executed} executed, {skipped} skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
