"""Validate adoption of the canonical AI-native platform standard."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

MANIFEST = Path("AI_NATIVE_PLATFORM.yaml")
STANDARD_REPOSITORY = "fatmambot33/ai-native-platform"
REQUIRED_TRUE_PATHS = (
    ("product", "typed"),
    ("product", "sdk"),
    ("product", "documented"),
    ("product", "install", "git"),
    ("product", "install", "pypi"),
    ("product", "install", "documented"),
    ("plugin", "enabled"),
    ("plugin", "codex"),
    ("plugin", "manifest"),
    ("plugin", "discovery"),
    ("installation", "git"),
    ("installation", "pypi"),
    ("installation", "documented"),
    ("self_improvement", "enabled"),
    ("self_improvement", "github", "issues_as_work_queue"),
    ("self_improvement", "agent_implementation", "enabled"),
    ("self_improvement", "agent_implementation", "ci_required"),
    ("release", "validate_against_standard"),
    ("release", "pin_standard"),
)
REQUIRED_GUARANTEES = {
    "deterministic_tool_discovery",
    "structured_outputs",
    "issue_driven_improvement",
    "ci_validated_changes",
    "governed_autonomy",
}
REQUIRED_APPROVALS = {
    "breaking_changes",
    "security_changes",
    "credential_changes",
    "public_api_changes",
    "release_changes",
    "permission_expansion",
}
REQUIRED_FILES = (
    Path(".github/ISSUE_TEMPLATE/ai-improvement.yml"),
    Path(".github/workflows/ai-self-improvement.yml"),
)


def read_path(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Read one nested manifest value."""
    current: Any = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            raise KeyError(".".join(path))
        current = current[key]
    return current


def main() -> int:
    """Validate the local adoption manifest and repository assets."""
    if not MANIFEST.is_file():
        print(f"Missing required manifest: {MANIFEST}")
        return 1

    data = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("version") != 1:
        print("Manifest version must be 1")
        return 1

    errors: list[str] = []
    platform = data.get("platform", {})
    if platform.get("standard_repository") != STANDARD_REPOSITORY:
        errors.append(f"platform.standard_repository must be {STANDARD_REPOSITORY}")
    if not platform.get("standard_ref"):
        errors.append("platform.standard_ref must pin an immutable revision")

    for path in REQUIRED_TRUE_PATHS:
        try:
            if read_path(data, path) is not True:
                errors.append(f"{'.'.join(path)} must be true")
        except KeyError:
            errors.append(f"missing {'.'.join(path)}")

    credentials = data.get("credentials", {})
    if credentials.get("required"):
        if credentials.get("storage") != "local_env_only":
            errors.append("credentials.storage must be local_env_only")
        for key in ("configure_command", "doctor_command"):
            if not credentials.get(key):
                errors.append(f"credentials.{key} is required")

    approvals = set(
        data.get("self_improvement", {})
        .get("governance", {})
        .get("human_approval_required", [])
    )
    missing_approvals = REQUIRED_APPROVALS - approvals
    if missing_approvals:
        errors.append(
            "missing human approval gates: "
            + ", ".join(sorted(missing_approvals))
        )

    guarantees = set(data.get("agent", {}).get("guarantees", []))
    missing_guarantees = REQUIRED_GUARANTEES - guarantees
    if missing_guarantees:
        errors.append(
            "missing agent guarantees: "
            + ", ".join(sorted(missing_guarantees))
        )

    for path in REQUIRED_FILES:
        if not path.is_file():
            errors.append(f"missing required self-improvement file: {path}")

    if errors:
        print("AI-native platform manifest validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("AI-native platform manifest is valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
