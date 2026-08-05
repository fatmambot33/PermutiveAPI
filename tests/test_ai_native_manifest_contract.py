"""Regression test for the AI-native platform manifest validator."""

from __future__ import annotations

import subprocess
import sys


def test_ai_native_manifest_validator_passes() -> None:
    """Ensure the checked-in manifest satisfies the local pinned contract."""
    result = subprocess.run(
        [sys.executable, "scripts/validate_ai_native_platform.py"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
