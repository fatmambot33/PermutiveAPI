"""Validate the repository against the pinned AI-native platform contract."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "AI_NATIVE_PLATFORM.yaml"
SCHEMA = ROOT / "schemas/ai-native-platform.schema.json"
IMMUTABLE_REF = re.compile(r"(?:[0-9a-f]{40}|v?\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)")
REQUIRED_GUARANTEES = {
    "deterministic_tool_discovery",
    "structured_outputs",
    "issue_driven_improvement",
    "ci_validated_changes",
    "governed_autonomy",
}
PROFILE_REQUIREMENTS: dict[str, tuple[str, ...]] = {
    "library": ("interfaces.sdk", "interfaces.json_schema"),
    "cli": ("interfaces.cli", "interfaces.json_schema"),
    "plugin": ("interfaces.plugin", "plugin.enabled", "interfaces.json_schema"),
    "agent-tool": ("interfaces.json_schema",),
    "service": ("interfaces.json_schema",),
    "full-platform": (
        "interfaces.sdk",
        "interfaces.cli",
        "interfaces.plugin",
        "plugin.enabled",
        "interfaces.json_schema",
    ),
}
BASE_EVIDENCE = {"readme", "tests", "agent_instructions", "typing", "ci"}


def read_path(data: Mapping[str, Any], dotted: str) -> Any:
    """Read one dotted path from a nested mapping."""
    value: Any = data
    for part in dotted.split("."):
        if not isinstance(value, Mapping) or part not in value:
            raise KeyError(dotted)
        value = value[part]
    return value


def required_evidence(data: Mapping[str, Any]) -> set[str]:
    """Return evidence keys implied by declared capabilities."""
    keys = set(BASE_EVIDENCE)
    interfaces = data.get("interfaces", {})
    quality = data.get("quality", {})
    plugin = data.get("plugin", {})
    improvement = data.get("self_improvement", {})

    if isinstance(interfaces, Mapping):
        for capability, evidence_key in {
            "sdk": "sdk",
            "cli": "cli",
            "json_schema": "schemas",
            "mcp": "mcp",
            "openapi": "openapi",
        }.items():
            if interfaces.get(capability) is True:
                keys.add(evidence_key)
        if interfaces.get("plugin") is True:
            keys.update({"plugin_manifest", "plugin_tests"})

    if isinstance(quality, Mapping):
        if quality.get("docs") is True:
            keys.add("docs")
        if quality.get("examples") is True:
            keys.add("examples")
        if quality.get("security_scan") is True:
            keys.add("security_workflow")

    if isinstance(plugin, Mapping):
        credentials = plugin.get("credentials", {})
        if isinstance(credentials, Mapping) and credentials.get("required") is True:
            keys.update({"env_example", "gitignore"})

    if isinstance(improvement, Mapping) and improvement.get("enabled") is True:
        keys.update({"self_improvement_workflow", "improvement_issue_template"})
    return keys


def declarations(value: Any) -> list[str]:
    """Normalize an evidence declaration to repository-relative paths."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [item for item in value if isinstance(item, str)]
    return []


def path_exists(declaration: str) -> bool:
    """Return whether one repository-relative path or glob exists."""
    path = Path(declaration)
    if path.is_absolute() or ".." in path.parts:
        return False
    if any(character in declaration for character in "*?["):
        return any(ROOT.glob(declaration))
    return (ROOT / declaration).exists()


def validate() -> list[str]:
    """Return deterministic contract and evidence errors."""
    data = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(schema, dict):
        return ["manifest and schema must contain mappings"]

    errors = []
    validator = Draft202012Validator(schema)
    for error in sorted(validator.iter_errors(data), key=lambda item: list(item.absolute_path)):
        location = ".".join(str(part) for part in error.absolute_path) or "manifest"
        errors.append(f"schema [{location}]: {error.message}")

    standard = data.get("standard", {})
    if isinstance(standard, Mapping):
        reference = str(standard.get("ref", ""))
        if IMMUTABLE_REF.fullmatch(reference) is None:
            errors.append("standard.ref must be an immutable version or commit SHA")

    product = data.get("product", {})
    profile = product.get("profile") if isinstance(product, Mapping) else None
    if isinstance(profile, str):
        for requirement in PROFILE_REQUIREMENTS.get(profile, ()):
            try:
                if read_path(data, requirement) is not True:
                    errors.append(f"{profile} requires {requirement}=true")
            except KeyError:
                errors.append(f"{profile} requires {requirement}")
        interfaces = data.get("interfaces", {})
        if isinstance(interfaces, Mapping):
            if profile == "agent-tool" and not (
                interfaces.get("plugin") is True or interfaces.get("mcp") is True
            ):
                errors.append("agent-tool requires a plugin or MCP interface")
            if profile == "service" and not (
                interfaces.get("openapi") is True or interfaces.get("sdk") is True
            ):
                errors.append("service requires an OpenAPI or SDK interface")

    agent = data.get("agent", {})
    guarantees = set(agent.get("guarantees", [])) if isinstance(agent, Mapping) else set()
    for guarantee in sorted(REQUIRED_GUARANTEES - guarantees):
        errors.append(f"missing agent guarantee: {guarantee}")

    evidence = data.get("evidence", {})
    paths = evidence.get("paths", {}) if isinstance(evidence, Mapping) else {}
    if not isinstance(paths, Mapping):
        errors.append("evidence.paths must be a mapping")
        return errors
    for key in sorted(required_evidence(data)):
        declared = declarations(paths.get(key))
        if not declared:
            errors.append(f"missing evidence declaration: {key}")
            continue
        missing = [item for item in declared if not path_exists(item)]
        if missing:
            errors.append(f"missing evidence for {key}: {', '.join(missing)}")
    return errors


def main() -> int:
    """Run validation and print an actionable result."""
    try:
        errors = validate()
    except (OSError, ValueError, json.JSONDecodeError, yaml.YAMLError) as exc:
        errors = [str(exc)]
    if errors:
        print("AI-native platform validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("AI-native platform validation passed with repository evidence.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
