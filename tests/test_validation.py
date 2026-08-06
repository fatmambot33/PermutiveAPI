"""Contract tests for local product validation and lifecycle commands."""

from __future__ import annotations

import json
from typing import List

import pytest

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


def test_test_command_runs_installed_package_checks(monkeypatch, capsys) -> None:
    """The test command reuses deterministic installed-package validation."""
    checks = [ValidationCheck("self_test", True, "self-test passed")]
    monkeypatch.setattr("PermutiveAPI.cli.run_validation", lambda: checks)

    assert main(["test"]) == 0
    output = capsys.readouterr().out
    assert "installed-package self-test" in output
    assert "[PASS] self_test" in output


def test_eval_command_prints_machine_readable_scorecard(capsys) -> None:
    """The eval command emits passing deterministic JSON evidence."""
    assert main(["eval"]) == 0

    scorecard = json.loads(capsys.readouterr().out)
    assert scorecard["schema_version"] == 1
    assert scorecard["summary"] == {
        "failed": 0,
        "ok": True,
        "passed": 11,
        "total": 11,
    }


@pytest.mark.parametrize(
    ("command", "expected"),
    (
        ("docs", "docs/AI_NATIVE.md"),
        ("examples", "PermutiveClient"),
        ("upgrade", "pip install --upgrade PermutiveAPI"),
        ("uninstall", "pip uninstall PermutiveAPI"),
    ),
)
def test_lifecycle_guidance_commands_are_deterministic(
    command: str,
    expected: str,
    capsys,
) -> None:
    """Lifecycle commands return stable, explicit guidance."""
    assert main([command]) == 0

    assert expected in capsys.readouterr().out
