"""Local credential, validation, and lifecycle commands for PermutiveAPI."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional, Sequence

from dotenv import dotenv_values

from .actionable_errors import classify_exception
from .client import PermutiveClient
from .credentials import CredentialsError, LocalCredentialsProvider
from .evaluations import run_default_evaluations
from .recipes import RecipeCategory, find_recipes
from .validation import run_validation, validation_succeeded

REQUIRED_VARIABLES = ("PERMUTIVE_API_KEY",)
DOCUMENTATION_PATHS = (
    "README.md",
    "docs/CLI.md",
    "docs/AI_NATIVE.md",
    "docs/AI_NATIVE_PLUGIN.md",
    "docs/EVALUATIONS.md",
    "docs/OPERATIONAL_RELIABILITY.md",
    "docs/MCP.md",
)
LIFECYCLE_COMMANDS = (
    "validate",
    "test",
    "eval",
    "docs",
    "examples",
    "upgrade",
    "uninstall",
)


def _is_ignored(env_path: Path) -> bool:
    """Return whether a nearby gitignore explicitly ignores ``.env``."""
    for parent in (env_path.parent, *env_path.parents):
        gitignore = parent / ".gitignore"
        if gitignore.is_file():
            entries = {
                line.strip()
                for line in gitignore.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            }
            return ".env" in entries or "*.env" in entries
        if (parent / ".git").exists():
            break
    return False


def _write_env(path: Path, values: Dict[str, str]) -> None:
    """Write credentials to a local file with restrictive permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "".join(f"{name}={value}\n" for name, value in values.items())
    path.write_text(content, encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _credential_source_label(source: str, env_file: Path) -> str:
    """Return a stable secret-free label for a credential source."""
    if source == "explicit":
        return "explicit"
    if source.startswith("environment:"):
        return "environment"
    if source.startswith("dotenv:"):
        source_path = Path(source.split(":", 1)[1]).expanduser()
        try:
            if source_path.resolve() == env_file.expanduser().resolve():
                return "project-dotenv"
        except OSError:
            pass
        return "user-dotenv"
    return "local"


def _project_env_problems(env_file: Path) -> list[str]:
    """Return security problems for the project dotenv file when it is used."""
    problems: list[str] = []
    if os.name != "nt" and env_file.stat().st_mode & 0o077:
        problems.append("credential file permissions are too broad; run chmod 600 .env")
    if not _is_ignored(env_file):
        problems.append(".env is not explicitly ignored by .gitignore")
    return problems


def connection_report(env_file: Path = Path(".env")) -> Dict[str, object]:
    """Check credential resolution and one safe read-only API connection."""
    provider = LocalCredentialsProvider(
        dotenv_paths=(env_file, Path.home() / ".config/permutive/.env")
    )
    try:
        credentials = provider.load()
    except CredentialsError:
        return {
            "ok": False,
            "credentials": {"status": "missing", "source": None},
            "connection": {"status": "not_checked"},
            "error_code": "credentials_missing",
            "retryable": False,
            "recommended_action": "Run `permutiveapi configure`, then run `permutiveapi check` again.",
            "safe_context": {},
        }

    source = _credential_source_label(credentials.source, env_file)
    if source == "project-dotenv":
        problems = _project_env_problems(env_file)
        if problems:
            return {
                "ok": False,
                "credentials": {
                    "status": "unsafe",
                    "source": source,
                    "problems": problems,
                },
                "connection": {"status": "not_checked"},
                "error_code": "credential_file_unsafe",
                "retryable": False,
                "recommended_action": "Run `permutiveapi doctor` and repair the local .env protections before connecting.",
                "safe_context": {},
            }

    try:
        with PermutiveClient(credentials.api_key) as client:
            client.cohorts.list(page_size=1)
    except Exception as exc:  # noqa: BLE001 - CLI boundary returns safe guidance
        guidance = classify_exception(exc, operation="connection_check")
        return {
            "ok": False,
            "credentials": {"status": "ok", "source": source},
            "connection": {"status": "failed"},
            "error_code": guidance.code,
            "retryable": guidance.retryable,
            "recommended_action": guidance.recommended_action,
            "safe_context": dict(guidance.safe_context),
        }

    return {
        "ok": True,
        "credentials": {"status": "ok", "source": source},
        "connection": {"status": "ok"},
        "error_code": None,
        "retryable": False,
        "recommended_action": None,
        "safe_context": {"operation": "connection_check"},
    }


def configure(env_file: Path, *, force: bool = False) -> int:
    """Interactively create a local credential file without echoing secrets."""
    if env_file.exists() and not force:
        print(f"Refusing to overwrite {env_file}. Use --force to replace it.")
        return 2

    print("PermutiveAPI local credential setup")
    print("Credentials remain in this local .env file and are never uploaded.")
    values = {
        "PERMUTIVE_API_KEY": getpass.getpass("Permutive API key: ").strip(),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        print("Missing required values: " + ", ".join(missing))
        return 2

    _write_env(env_file, values)
    print(f"Created {env_file} with local-only permissions where supported.")
    if not _is_ignored(env_file):
        print("WARNING: add .env to the repository .gitignore before committing.")
        return 1
    print("Credential setup complete. Run `permutiveapi check` to verify it.")
    return 0


def doctor(env_file: Path) -> int:
    """Validate local credential setup without exposing secret values."""
    problems = []
    if not env_file.is_file():
        problems.append(f"missing credential file: {env_file}")
        values: Dict[str, Optional[str]] = {}
    else:
        values = dict(dotenv_values(env_file))
        missing = [name for name in REQUIRED_VARIABLES if not values.get(name)]
        if missing:
            problems.append("missing variables: " + ", ".join(missing))
        if os.name != "nt" and env_file.stat().st_mode & 0o077:
            problems.append(
                "credential file permissions are too broad; run chmod 600 .env"
            )
    if not _is_ignored(env_file):
        problems.append(".env is not explicitly ignored by .gitignore")

    if problems:
        print("PermutiveAPI credential check failed:")
        for problem in problems:
            print(f"- {problem}")
        print("Run `permutiveapi configure` to repair local configuration.")
        return 1

    print("PermutiveAPI local credential check passed.")
    print("Required variables are present; values were not displayed.")
    return 0


def check(env_file: Path, *, as_json: bool = False) -> int:
    """Check credentials and live API connectivity with one safe command."""
    report = connection_report(env_file)
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["ok"] else 1

    credentials = report["credentials"]
    connection = report["connection"]
    assert isinstance(credentials, dict)
    assert isinstance(connection, dict)
    print("PermutiveAPI readiness check")
    source = credentials.get("source")
    source_text = f" ({source})" if source else ""
    print(f"- credentials: {credentials.get('status')}{source_text}")
    print(f"- connection: {connection.get('status')}")
    if report["ok"]:
        print("PermutiveAPI is ready.")
        return 0
    print(f"- {report['error_code']}: {report['recommended_action']}")
    return 1


def validate() -> int:
    """Validate the installed product surface without requiring credentials."""
    results = run_validation()
    print("PermutiveAPI product validation")
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"- [{status}] {result.name}: {result.detail}")
    if validation_succeeded(results):
        print("PermutiveAPI product validation passed.")
        return 0
    print("PermutiveAPI product validation failed.")
    return 1


def test() -> int:
    """Run the deterministic installed-package self-test."""
    print("PermutiveAPI installed-package self-test")
    return validate()


def evaluate() -> int:
    """Print the deterministic governed-platform evaluation scorecard."""
    scorecard = run_default_evaluations()
    print(scorecard.to_json(), end="")
    return 0 if scorecard.ok else 1


def docs() -> int:
    """Print the canonical documentation locations."""
    print("PermutiveAPI documentation")
    for path in DOCUMENTATION_PATHS:
        print(f"- {path}")
    print("Repository: https://github.com/fatmambot33/PermutiveAPI")
    return 0


def examples(
    *,
    category: str | None = None,
    name: str | None = None,
    as_json: bool = False,
) -> int:
    """List or print canonical executable recipes."""
    recipes = find_recipes(category=category, name=name)
    if not recipes:
        print("No recipe matched the requested category and name.")
        return 2
    if as_json:
        print(
            json.dumps(
                [recipe.to_dict() for recipe in recipes],
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if name is not None:
        print(recipes[0].source, end="")
        return 0
    print("PermutiveAPI executable recipes")
    for recipe in recipes:
        local = "credential-free" if recipe.credential_free else "credentials required"
        print(
            f"- [{recipe.category.value}] {recipe.name}: "
            f"{recipe.description} ({local})"
        )
    print("Print one recipe with `permutiveapi examples --name <name>`.")
    return 0


def upgrade() -> int:
    """Print the explicit environment-safe package upgrade command."""
    print("Upgrade PermutiveAPI explicitly with:")
    print(f"{sys.executable} -m pip install --upgrade PermutiveAPI")
    return 0


def uninstall() -> int:
    """Print the explicit environment-safe package removal command."""
    print("Uninstall PermutiveAPI explicitly with:")
    print(f"{sys.executable} -m pip uninstall PermutiveAPI")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(prog="permutiveapi")
    subparsers = parser.add_subparsers(dest="command", required=True)
    configure_parser = subparsers.add_parser("configure")
    configure_parser.add_argument("--env-file", type=Path, default=Path(".env"))
    configure_parser.add_argument("--force", action="store_true")
    doctor_parser = subparsers.add_parser("doctor")
    doctor_parser.add_argument("--env-file", type=Path, default=Path(".env"))
    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--env-file", type=Path, default=Path(".env"))
    check_parser.add_argument("--json", action="store_true")
    for command in LIFECYCLE_COMMANDS:
        if command != "examples":
            subparsers.add_parser(command)
    examples_parser = subparsers.add_parser("examples")
    examples_parser.add_argument(
        "--category",
        choices=[category.value for category in RecipeCategory],
    )
    examples_parser.add_argument("--name")
    examples_parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the PermutiveAPI command-line interface."""
    args = build_parser().parse_args(argv)
    if args.command == "configure":
        return configure(args.env_file, force=args.force)
    if args.command == "doctor":
        return doctor(args.env_file)
    if args.command == "check":
        return check(args.env_file, as_json=args.json)
    if args.command == "examples":
        return examples(
            category=args.category,
            name=args.name,
            as_json=args.json,
        )
    commands = {
        "validate": validate,
        "test": test,
        "eval": evaluate,
        "docs": docs,
        "upgrade": upgrade,
        "uninstall": uninstall,
    }
    return commands[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
