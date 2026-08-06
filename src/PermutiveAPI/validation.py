"""Local product validation for the installed PermutiveAPI package."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata
from typing import Callable, List, Sequence


@dataclass(frozen=True)
class ValidationCheck:
    """Represent one deterministic, secret-free validation check.

    Parameters
    ----------
    name:
        Stable machine-readable check name.
    passed:
        Whether the check passed.
    detail:
        Human-readable result without credential or payload values.
    """

    name: str
    passed: bool
    detail: str


CheckRunner = Callable[[], ValidationCheck]


def _package_metadata_check() -> ValidationCheck:
    """Validate installed package metadata."""
    try:
        version = metadata.version("PermutiveAPI")
    except metadata.PackageNotFoundError:
        return ValidationCheck(
            name="package_metadata",
            passed=False,
            detail="PermutiveAPI distribution metadata is unavailable.",
        )
    if not version:
        return ValidationCheck(
            name="package_metadata",
            passed=False,
            detail="PermutiveAPI version metadata is empty.",
        )
    return ValidationCheck(
        name="package_metadata",
        passed=True,
        detail=f"PermutiveAPI {version} metadata is available.",
    )


def _plugin_entry_point_check() -> ValidationCheck:
    """Validate the built-in Python plugin entry point."""
    entry_points = metadata.entry_points()
    if hasattr(entry_points, "select"):
        candidates = entry_points.select(group="permutiveapi.plugins", name="codex")
    else:  # pragma: no cover - compatibility for older importlib metadata APIs
        candidates = [
            entry_point
            for entry_point in entry_points.get("permutiveapi.plugins", [])
            if entry_point.name == "codex"
        ]
    matches = list(candidates)
    if len(matches) != 1:
        return ValidationCheck(
            name="plugin_entry_point",
            passed=False,
            detail="Expected exactly one permutiveapi.plugins codex entry point.",
        )
    try:
        plugin_type = matches[0].load()
    except (ImportError, AttributeError, ModuleNotFoundError) as error:
        return ValidationCheck(
            name="plugin_entry_point",
            passed=False,
            detail=f"Codex plugin entry point could not load: {type(error).__name__}.",
        )
    if plugin_type.__name__ != "CodexPlugin":
        return ValidationCheck(
            name="plugin_entry_point",
            passed=False,
            detail="Codex plugin entry point resolved to an unexpected type.",
        )
    return ValidationCheck(
        name="plugin_entry_point",
        passed=True,
        detail="Codex plugin entry point resolves deterministically.",
    )


def _public_tools_check() -> ValidationCheck:
    """Validate that public OpenAI tool schemas are available."""
    try:
        from PermutiveAPI.tools import ToolRegistry
    except ImportError as error:
        return ValidationCheck(
            name="public_tools",
            passed=False,
            detail=f"Tool registry import failed: {type(error).__name__}.",
        )
    required_methods = ("as_openai_tools", "capabilities")
    missing = [name for name in required_methods if not hasattr(ToolRegistry, name)]
    if missing:
        return ValidationCheck(
            name="public_tools",
            passed=False,
            detail="Tool registry is missing required public methods: "
            + ", ".join(missing),
        )
    return ValidationCheck(
        name="public_tools",
        passed=True,
        detail="Public tool schema and capability methods are available.",
    )


def run_validation(
    checks: Sequence[CheckRunner] | None = None,
) -> List[ValidationCheck]:
    """Run deterministic local product validation.

    Parameters
    ----------
    checks:
        Optional check runners for deterministic testing.

    Returns
    -------
    list of ValidationCheck
        Ordered validation results.
    """
    runners = checks or (
        _package_metadata_check,
        _plugin_entry_point_check,
        _public_tools_check,
    )
    return [runner() for runner in runners]


def validation_succeeded(results: Sequence[ValidationCheck]) -> bool:
    """Return whether every validation check passed."""
    return bool(results) and all(result.passed for result in results)
