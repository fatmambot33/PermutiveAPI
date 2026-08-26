"""Regression tests for the vendored AI-native platform contract."""

from __future__ import annotations

import ast
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts" / "validate_ai_native_platform.py"
BASE_EVIDENCE = {"readme", "tests", "agent_instructions", "typing", "ci"}


def _required_evidence() -> Callable[[Mapping[str, Any]], set[str]]:
    """Load the vendored ``required_evidence`` function without tool dependencies."""
    tree = ast.parse(VALIDATOR.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "required_evidence"
    )
    module = ast.Module(body=[function], type_ignores=[])
    namespace: dict[str, Any] = {
        "Any": Any,
        "Mapping": Mapping,
        "BASE_EVIDENCE": BASE_EVIDENCE,
    }
    exec(compile(module, str(VALIDATOR), "exec"), namespace)
    loaded = namespace["required_evidence"]
    assert callable(loaded)
    return loaded


def test_security_evidence_is_accepted() -> None:
    """Accept the canonical generic security evidence key."""
    required = _required_evidence()({"quality": {"security_scan": True}})
    declared = BASE_EVIDENCE | {"security_evidence"}

    assert required <= declared


def test_security_workflow_is_not_a_compatibility_alias() -> None:
    """Reject the removed workflow-specific key as security evidence."""
    required = _required_evidence()({"quality": {"security_scan": True}})
    declared = BASE_EVIDENCE | {"security_workflow"}

    assert "security_evidence" in required
    assert "security_workflow" not in required
    assert not required <= declared
