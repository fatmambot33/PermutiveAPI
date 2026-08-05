"""Validate declarations and concrete AI-native repository evidence."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

import yaml

ROOT = Path(".")
MANIFEST = ROOT / "AI_NATIVE_PLATFORM.yaml"
STANDARD_REPOSITORY = "fatmambot33/ai-native-platform"
REQUIRED_TRUE_PATHS = (
    ("product", "ai_native"), ("product", "plugin_first"), ("product", "sdk_first"),
    ("plugin", "enabled"), ("plugin", "codex", "supported"),
    ("plugin", "codex", "marketplace"), ("plugin", "discovery", "entry_points"),
    ("plugin", "discovery", "manifest"), ("plugin", "discovery", "capabilities"),
    ("plugin", "credentials", "local_only"),
    ("plugin", "credentials", "policy", "never_store_remote"),
    ("plugin", "credentials", "policy", "never_commit"),
    ("plugin", "credentials", "policy", "never_echo"),
    ("interfaces", "sdk"), ("interfaces", "cli"), ("interfaces", "plugin"),
    ("interfaces", "json_schema"), ("quality", "typed"), ("quality", "tests"),
    ("quality", "docs"), ("quality", "examples"), ("quality", "security_scan"),
    ("self_improvement", "enabled"), ("self_improvement", "github", "issues"),
    ("self_improvement", "autonomous", "discover_improvements"),
    ("self_improvement", "autonomous", "create_issues"),
    ("self_improvement", "autonomous", "generate_pr"),
    ("self_improvement", "autonomous", "run_ci"),
    ("release", "block_if_quality_fails"), ("release", "block_if_plugin_invalid"),
)
REQUIRED_COMMANDS = {"validate", "test", "docs", "examples", "upgrade", "uninstall"}
REQUIRED_GUARANTEES = {"deterministic_tool_discovery", "structured_outputs", "issue_driven_improvement", "ci_validated_changes", "governed_autonomy"}
REQUIRED_APPROVALS = {"breaking_changes", "security_changes", "credential_changes", "public_api_changes", "permission_expansion", "release_changes"}


def read_path(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Read one nested manifest value."""
    current: Any = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            raise KeyError(".".join(path))
        current = current[key]
    return current


def matching(patterns: Iterable[str]) -> list[Path]:
    """Return matching repository files."""
    return sorted({path for pattern in patterns for path in ROOT.glob(pattern) if path.is_file() and ".git" not in path.parts})


def text(paths: Iterable[Path]) -> str:
    """Read small text files as lowercase text."""
    return "\n".join(path.read_text(encoding="utf-8", errors="ignore") for path in paths if path.stat().st_size < 2_000_000).lower()


def evidence(data: dict[str, Any]) -> list[str]:
    """Return missing evidence labels."""
    readmes = matching(("README.md", "docs/**/*.md"))
    source = matching(("src/**/*.py", "**/plugins/**/*.py", "plugins/**/*.py"))
    workflows = matching((".github/workflows/*.yml", ".github/workflows/*.yaml"))
    tests = matching(("tests/test_*.py", "tests/**/*test*.py"))
    docs = text(readmes)
    code = text(source)
    ci = text(workflows)
    checks = {
        "pyproject.toml": (ROOT / "pyproject.toml").is_file(),
        "Codex plugin manifest": bool(matching((".codex-plugin/plugin.json", "plugins/**/.codex-plugin/plugin.json"))),
        "Codex marketplace catalog": bool(matching((".agents/plugins/marketplace.json", "plugins/**/marketplace.json"))),
        "typed plugin contract": "plugin" in code and ("protocol" in code or "abstractbaseclass" in code),
        "typing marker or explicit Pyright contract": bool(matching(("src/**/py.typed", "**/py.typed"))) or "pyright" in text([ROOT / "pyproject.toml"]),
        "strict type checking in CI": "pyright" in ci or "mypy" in ci,
        "plugin tests": any("plugin" in path.name.lower() for path in tests),
        "general tests": bool(tests),
        "AGENTS.md": (ROOT / "AGENTS.md").is_file(),
        "PyPI installation documentation": "pip install" in docs,
        "Git installation documentation": "git+https://" in docs or "git clone" in docs,
        "editable installation documentation": "pip install -e" in docs,
        "plugin documentation": "plugin" in docs and "codex" in docs,
        "AI improvement issue template": (ROOT / ".github/ISSUE_TEMPLATE/ai-improvement.yml").is_file(),
        "self-improvement workflow": (ROOT / ".github/workflows/ai-self-improvement.yml").is_file() or (ROOT / ".github/workflows/ai-self-improve.yml").is_file(),
    }
    credentials = data.get("plugin", {}).get("credentials", {})
    if credentials.get("required"):
        checks["credential template"] = isinstance(credentials.get("env_example"), str) and (ROOT / credentials["env_example"]).is_file()
        checks["configure command"] = bool(credentials.get("setup_command"))
        checks["doctor command"] = bool(credentials.get("validation_command"))
        checks[".env ignored"] = (ROOT / ".gitignore").is_file() and ".env" in (ROOT / ".gitignore").read_text(encoding="utf-8", errors="ignore")
    return [label for label, passed in checks.items() if not passed]


def main() -> int:
    """Validate the local manifest and repository evidence."""
    data = yaml.safe_load(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.is_file() else None
    if not isinstance(data, dict) or data.get("version") != 1:
        print("AI-native platform validation failed:\n- missing or invalid version 1 manifest")
        return 1
    errors: list[str] = []
    standard = data.get("standard", {})
    if standard.get("repository") != STANDARD_REPOSITORY:
        errors.append(f"standard.repository must be {STANDARD_REPOSITORY}")
    if not re.fullmatch(r"[0-9a-f]{40}|v?\d+\.\d+\.\d+", str(standard.get("ref", ""))):
        errors.append("standard.ref must pin an immutable commit or semantic version")
    for path in REQUIRED_TRUE_PATHS:
        try:
            if read_path(data, path) is not True:
                errors.append(f"{'.'.join(path)} must be true")
        except KeyError:
            errors.append(f"missing {'.'.join(path)}")
    commands = set(data.get("commands", {}).get("required", []))
    errors.extend(f"commands.required must include {item}" for item in sorted(REQUIRED_COMMANDS - commands))
    credentials = data.get("plugin", {}).get("credentials", {})
    if credentials.get("required"):
        errors.extend(f"plugin.credentials.{field} is required" for field in ("env_file", "env_example", "setup_command", "validation_command") if not credentials.get(field))
        errors.extend(f"credentialed products require command {item}" for item in ("configure", "doctor") if item not in commands)
    approvals = data.get("self_improvement", {}).get("governance", {}).get("human_approval", {})
    missing_approvals = sorted(name for name in REQUIRED_APPROVALS if approvals.get(name) is not True)
    if missing_approvals:
        errors.append("missing human approval gates: " + ", ".join(missing_approvals))
    missing_guarantees = sorted(REQUIRED_GUARANTEES - set(data.get("agent", {}).get("guarantees", [])))
    if missing_guarantees:
        errors.append("missing agent guarantees: " + ", ".join(missing_guarantees))
    errors.extend(f"missing repository evidence: {label}" for label in evidence(data))
    if errors:
        print("AI-native platform validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("AI-native platform validation passed with repository evidence.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
