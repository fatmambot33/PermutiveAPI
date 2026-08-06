"""Tests for deterministic governed-platform evaluations."""

from __future__ import annotations

import json
from pathlib import Path

from PermutiveAPI.evaluations import (
    EvaluationCase,
    EvaluationObservation,
    default_evaluation_cases,
    run_default_evaluations,
    run_evaluations,
)


def test_default_scorecard_is_complete_and_deterministic() -> None:
    """The canonical scorecard is stable and every safety case passes."""
    first = run_default_evaluations()
    second = run_default_evaluations()

    assert first.ok is True
    assert first.total == 11
    assert first.passed == 11
    assert first.failed == 0
    assert first.to_json() == second.to_json()
    assert [result.name for result in first.results] == [
        "tool_selection",
        "unsupported_capability",
        "read_without_approval",
        "write_requires_approval",
        "allow_list_enforced",
        "deny_list_enforced",
        "failure_redaction",
        "idempotent_write",
        "workflow_bound",
        "partial_failure",
        "audit_completeness",
    ]


def test_committed_scorecard_matches_runtime_evidence() -> None:
    """Machine-readable evaluation evidence cannot drift from the implementation."""
    committed = Path("evals/scorecard.json").read_text(encoding="utf-8")

    assert committed == run_default_evaluations().to_json()
    assert json.loads(committed)["summary"] == {
        "failed": 0,
        "ok": True,
        "passed": 11,
        "total": 11,
    }


def test_runner_failure_does_not_expose_exception_message() -> None:
    """Evaluation infrastructure preserves failure type without leaking text."""

    def leak() -> EvaluationObservation:
        raise RuntimeError("evaluation-secret-must-not-escape")

    scorecard = run_evaluations((EvaluationCase("redaction", "security", leak),))
    serialized = scorecard.to_json()

    assert scorecard.ok is False
    assert "RuntimeError" in serialized
    assert "evaluation-secret-must-not-escape" not in serialized


def test_default_cases_have_unique_stable_names() -> None:
    """Scorecard case identifiers remain unique and machine readable."""
    names = [case.name for case in default_evaluation_cases()]

    assert len(names) == len(set(names))
    assert all(name.replace("_", "").isalnum() for name in names)
