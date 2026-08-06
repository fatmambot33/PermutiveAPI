"""Local credential, validation, and lifecycle commands for PermutiveAPI."""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from pathlib import Path
from typing import Dict, Optional, Sequence

from dotenv import dotenv_values

from .evaluations import run_default_evaluations
from .validation import run_validation, validation_succeeded

REQUIRED_VARIABLES = ("PERMUTIVE_API_KEY",)
DOCUMENTATION_PATHS = (
    "README.md",
    "docs/CLI.md",
    "docs/AI_NATIVE.md",
    "docs/AI_NATIVE_PLUGIN.md",
    "docs/EVALUATIONS.md",
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
    print("Credential setup complete. Run `permutiveapi doctor` to verify it.")
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


def examples() -> int:
    """Print minimal canonical SDK and plugin examples."""
    print("PermutiveAPI SDK example")
    print("from PermutiveAPI import PermutiveClient")
    print('client = PermutiveClient("api-key")')
    print('cohort = client.cohorts.get("cohort-id")')
    print()
    print("PermutiveAPI Codex plugin example")
    print("from PermutiveAPI.plugins.codex import CodexPlugin")
    print("plugin = CodexPlugin.from_env()")
    print("tools = plugin.tools().as_openai_tools()")
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
    for command in LIFECYCLE_COMMANDS:
        subparsers.add_parser(command)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the PermutiveAPI command-line interface."""
    args = build_parser().parse_args(argv)
    if args.command == "configure":
        return configure(args.env_file, force=args.force)
    if args.command == "doctor":
        return doctor(args.env_file)
    commands = {
        "validate": validate,
        "test": test,
        "eval": evaluate,
        "docs": docs,
        "examples": examples,
        "upgrade": upgrade,
        "uninstall": uninstall,
    }
    return commands[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
