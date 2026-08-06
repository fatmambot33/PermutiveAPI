"""Contract tests for the explicit package typing boundary."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


def _scope() -> Dict[str, object]:
    """Load the machine-readable typing scope."""
    return json.loads(Path("TYPING_SCOPE.json").read_text(encoding="utf-8"))


def test_every_package_module_has_exactly_one_typing_classification() -> None:
    """New modules cannot silently escape strict or compatibility review."""
    scope = _scope()
    strict = set(scope["strict"])
    compatibility = set(scope["compatibility"])
    discovered = {
        path.as_posix()
        for path in Path("src/PermutiveAPI").rglob("*.py")
        if "__pycache__" not in path.parts
    }

    assert strict.isdisjoint(compatibility)
    assert strict | compatibility == discovered


def test_typing_scope_uses_existing_files_and_pep561_marker() -> None:
    """The declared scope and distribution typing marker remain concrete."""
    scope = _scope()
    classified: List[str] = list(scope["strict"]) + list(scope["compatibility"])

    assert scope["version"] == 1
    assert all(Path(path).is_file() for path in classified)
    assert Path("src/PermutiveAPI/py.typed").is_file()


def test_compatibility_scope_is_explicit_and_bounded() -> None:
    """Legacy exclusions cannot grow without an intentional contract change."""
    compatibility = set(_scope()["compatibility"])

    assert compatibility == {
        "src/PermutiveAPI/audience/__init__.py",
        "src/PermutiveAPI/audience/imports.py",
        "src/PermutiveAPI/audience/segment.py",
        "src/PermutiveAPI/audience/source.py",
        "src/PermutiveAPI/cohort.py",
        "src/PermutiveAPI/context.py",
        "src/PermutiveAPI/identify/__init__.py",
        "src/PermutiveAPI/identify/alias.py",
        "src/PermutiveAPI/identify/identify.py",
        "src/PermutiveAPI/query.py",
        "src/PermutiveAPI/segmentation.py",
        "src/PermutiveAPI/utils/file.py",
        "src/PermutiveAPI/utils/http.py",
        "src/PermutiveAPI/utils/json.py",
        "src/PermutiveAPI/utils/list.py",
        "src/PermutiveAPI/workspace.py",
    }
