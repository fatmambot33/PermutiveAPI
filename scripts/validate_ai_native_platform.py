"""Validate adoption of the canonical AI-native platform standard."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

MANIFEST = Path("AI_NATIVE_PLATFORM.yaml")
STANDARD_REPOSITORY = "fatmambot33/ai-native-platform"
REQUIRED_TRUE_PATHS = (
    ("product", "ai_native"),
    ("product", "plugin_first"),
    ("product", "sdk_first"),
    ("plugin", "enabled"),
    ("plugin", "codex", "supported"),
    ("plugin", "codex", "marketplace"),
    ("plugin", "discovery", "entry_points"),
    ("plugin", "discovery", "manifest"),
    ("plugin", "credentials", "local_only"),
    ("plugin", "credentials", "policy", "never_store_remote"),
    ("plugin", "credentials", "policy", "never_commit"),
    ("plugin", "credentials", "policy", "never_echo"),
    ("interfaces", "sdk"),
    ("interfaces", "cli"),
    ("interfaces", "plugin"),
    ("quality", "typed"),
    ("quality", "tests"),
    ("quality", "docs"),
    ("self_improvement", "enabled"),
    ("self_improvement", "github", "issues"),
    ("self_improvement", "autonomous", "discover_improvements"),
    ("self_improvement", "autonomous", "create_issues"),
    ("self_improvement", "autonomous", "generate_pr"),
    ("self_improvement", "autonomous", "run_ci"),
    ("self_improvement", "autonomous", "update_roadmap_and_changelog"),
    ("release", "block_if_quality_fails"),
    ("release", "block_if_plugin_invalid"),
    ("release", "block_if_self_improvement_contract_invalid"),
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
    standard = data.get("standard", {})
    if standard.get("repository") != STANDARD_REPOSITORY:
        errors.append(f"standard.repository must be {STANDARD_REPOSITORY}")
    if not standard.get("ref"):
        errors.append("standard.ref must pin an immutable revision")

    for path in REQUIRED_TRUE_PATHS:
        try:
            if read_path(data, path) is not True:
                errors.append(f"{'.'.join(path)} must be true")
        except KeyError:
            errors.append(f"missing {'.'.join(path)}")

    commands = set(data.get("commands", {}).get("required", []))
    for command in ("validate", "test", "docs", "examples", "upgrade", "uninstall"):
        if command not in commands:
            errors.append(f"commands.required must include {command}")

    credentials = data.get("plugin", {}).get("credentials", {})
    if credentials.get("required"):
        for key in ("env_file", "env_example", "setup_command", "validation_command"):
            if not credentials.get(key):
                errors.append(f"plugin.credentials.{key} is required")
        if not credentials.get("required_variables"):
            errors.append("plugin.credentials.required_variables must not be empty")
        for command in ("configure", "doctor"):
            if command not in commands:
                errors.append(f"credentialed products require command {command}")

    approvals = data.get("self_improvement", {}).get("governance", {}).get(
        "human_approval", {}
    )
    missing_approvals = {
        name for name in REQUIRED_APPROVALS if approvals.get(name) is not True
    }
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
