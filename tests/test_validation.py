"""Contract tests for local product validation."""

from __future__ import annotations

from typing import List

from PermutiveAPI.cli import main
from PermutiveAPI.validation import (
    ValidationCheck,
    run_validation,
    validation_succeeded,
)


def test_run_validation_preserves_check_order() -> None:
    """Validation results remain deterministic."""

    def first() -> ValidationCheck:
        return ValidationCheck("first", True, "first passed")

    def second() -> ValidationCheck:
        return ValidationCheck("second", False, "second failed")

    results = run_validation((first, second))

    assert [result.name for result in results] == ["first", "second"]
    assert validation_succeeded(results) is False


def test_validation_requires_at_least_one_check() -> None:
    """An empty check set cannot report a false positive."""
    results: List[ValidationCheck] = []

    assert validation_succeeded(results) is False


def test_validate_cli_reports_passes(monkeypatch, capsys) -> None:
    """The validate command returns zero when every check passes."""
    checks = [ValidationCheck("contract", True, "contract is valid")]
    monkeypatch.setattr("PermutiveAPI.cli.run_validation", lambda: checks)

    assert main(["validate"]) == 0
    output = capsys.readouterr().out
    assert "[PASS] contract" in output
    assert "product validation passed" in output


def test_validate_cli_reports_failures(monkeypatch, capsys) -> None:
    """The validate command returns one with actionable failure output."""
    checks = [ValidationCheck("contract", False, "contract is invalid")]
    monkeypatch.setattr("PermutiveAPI.cli.run_validation", lambda: checks)

    assert main(["validate"]) == 1
    output = capsys.readouterr().out
    assert "[FAIL] contract" in output
    assert "contract is invalid" in output
    assert "product validation failed" in output
