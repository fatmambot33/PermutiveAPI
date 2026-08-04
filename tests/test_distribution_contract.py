"""Tests for the built wheel distribution contract."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from zipfile import ZipFile


def test_wheel_contains_typed_package_and_plugin_metadata(tmp_path: Path) -> None:
    """Ensure the wheel ships the supported package and discovery metadata."""
    project_root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "dist"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(output_dir),
        ],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )

    wheels = list(output_dir.glob("*.whl"))
    assert len(wheels) == 1

    with ZipFile(wheels[0]) as wheel:
        names = set(wheel.namelist())
        entry_points_name = next(
            name for name in names if name.endswith(".dist-info/entry_points.txt")
        )
        entry_points = wheel.read(entry_points_name).decode("utf-8")

    required_package_files = {
        "PermutiveAPI/__init__.py",
        "PermutiveAPI/agent.py",
        "PermutiveAPI/client.py",
        "PermutiveAPI/mcp.py",
        "PermutiveAPI/py.typed",
        "PermutiveAPI/resources.py",
        "PermutiveAPI/sdk.py",
        "PermutiveAPI/tools.py",
        "PermutiveAPI/plugins/__init__.py",
        "PermutiveAPI/plugins/codex.py",
    }
    assert required_package_files <= names
    assert not any(name.startswith("tests/") for name in names)
    assert not any(name.startswith("docs/") for name in names)
    assert "[permutiveapi.plugins]" in entry_points
    assert "codex = PermutiveAPI.plugins.codex:CodexPlugin" in entry_points
