"""Validate local release metadata without network access."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

VERSION_PATTERN = re.compile(
    r'^version\s*=\s*"(?P<version>[^\"]+)"\s*$',
    re.MULTILINE,
)
CHANGELOG_PATTERN = r"^## {version} - \d{{4}}-\d{{2}}-\d{{2}}$"


def project_version(pyproject: Path) -> str:
    """Read the project version from pyproject.toml."""
    text = pyproject.read_text(encoding="utf-8")
    project_start = text.find("[project]")
    if project_start < 0:
        raise ValueError("pyproject.toml has no [project] table")
    next_table = text.find("\n[", project_start + len("[project]"))
    project_text = text[project_start : next_table if next_table >= 0 else None]
    match = VERSION_PATTERN.search(project_text)
    if match is None:
        raise ValueError("pyproject.toml [project] table has no version")
    return match.group("version")


def validate(root: Path, expected_tag: str | None = None) -> list[str]:
    """Return release metadata consistency errors."""
    errors: list[str] = []
    pyproject = root / "pyproject.toml"
    changelog = root / "CHANGELOG.md"
    if not pyproject.is_file():
        return ["missing pyproject.toml"]
    if not changelog.is_file():
        return ["missing CHANGELOG.md"]
    try:
        version = project_version(pyproject)
    except ValueError as error:
        return [str(error)]

    changelog_text = changelog.read_text(encoding="utf-8")
    heading = re.compile(
        CHANGELOG_PATTERN.format(version=re.escape(version)),
        re.MULTILINE,
    )
    if heading.search(changelog_text) is None:
        errors.append(
            f"CHANGELOG.md must contain a dated `## {version} - YYYY-MM-DD` heading"
        )

    release_notes = root / "docs" / "releases" / f"{version}.md"
    if not release_notes.is_file():
        errors.append(f"missing release notes: {release_notes.as_posix()}")

    if expected_tag is not None:
        normalized_tag = expected_tag.removeprefix("refs/tags/").removeprefix("v")
        if normalized_tag != version:
            errors.append(
                f"tag version {normalized_tag!r} does not match project version {version!r}"
            )
    return errors


def main() -> int:
    """Run release metadata validation."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--tag")
    args = parser.parse_args()
    errors = validate(args.root, args.tag)
    if errors:
        print("Release metadata validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    version = project_version(args.root / "pyproject.toml")
    print(f"Release metadata validation passed for PermutiveAPI {version}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
