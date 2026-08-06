"""Validate the explicit strict-typing and compatibility module boundary."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(".")
SCOPE_PATH = ROOT / "TYPING_SCOPE.json"
PACKAGE_ROOT = ROOT / "src" / "PermutiveAPI"
PYPROJECT_PATH = ROOT / "pyproject.toml"


def _string_set(data: dict[str, Any], key: str) -> set[str]:
    """Return one validated string-list field as a set."""
    values = data.get(key)
    if not isinstance(values, list) or not all(
        isinstance(value, str) for value in values
    ):
        raise ValueError(f"TYPING_SCOPE.json field {key!r} must be a string list")
    return set(values)


def validate() -> list[str]:
    """Return typing-scope consistency errors."""
    errors: list[str] = []
    if not SCOPE_PATH.is_file():
        return ["missing TYPING_SCOPE.json"]
    data = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("version") != 1:
        return ["TYPING_SCOPE.json must be a version 1 object"]
    try:
        strict = _string_set(data, "strict")
        compatibility = _string_set(data, "compatibility")
    except ValueError as error:
        return [str(error)]

    overlap = strict & compatibility
    if overlap:
        errors.append(
            "modules cannot be both strict and compatibility: "
            + ", ".join(sorted(overlap))
        )

    discovered = {
        path.relative_to(ROOT).as_posix()
        for path in PACKAGE_ROOT.rglob("*.py")
        if "__pycache__" not in path.parts
    }
    classified = strict | compatibility
    missing = discovered - classified
    stale = classified - discovered
    if missing:
        errors.append("unclassified package modules: " + ", ".join(sorted(missing)))
    if stale:
        errors.append(
            "typing scope references missing modules: " + ", ".join(sorted(stale))
        )

    pyproject = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))
    pyright = pyproject.get("tool", {}).get("pyright", {})
    configured = pyright.get("include", [])
    if not isinstance(configured, list) or not all(
        isinstance(value, str) for value in configured
    ):
        errors.append("tool.pyright.include must be a string list")
    elif set(configured) != strict:
        omitted = strict - set(configured)
        unexpected = set(configured) - strict
        if omitted:
            errors.append(
                "strict modules omitted from Pyright: " + ", ".join(sorted(omitted))
            )
        if unexpected:
            errors.append(
                "Pyright includes undeclared modules: " + ", ".join(sorted(unexpected))
            )

    if pyright.get("typeCheckingMode") != "strict":
        errors.append("tool.pyright.typeCheckingMode must be strict")
    if not (PACKAGE_ROOT / "py.typed").is_file():
        errors.append("missing PEP 561 marker: src/PermutiveAPI/py.typed")
    return errors


def main() -> int:
    """Run typing-scope validation."""
    errors = validate()
    if errors:
        print("Typing scope validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Typing scope validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
