"""Local-only credential setup and validation commands for PermutiveAPI."""

from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path
from typing import Dict, Optional, Sequence

from dotenv import dotenv_values

from .validation import run_validation, validation_succeeded

REQUIRED_VARIABLES = ("PERMUTIVE_API_KEY", "PERMUTIVE_WORKSPACE_ID")


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
        "PERMUTIVE_WORKSPACE_ID": input("Permutive workspace ID: ").strip(),
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


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(prog="permutiveapi")
    subparsers = parser.add_subparsers(dest="command", required=True)
    configure_parser = subparsers.add_parser("configure")
    configure_parser.add_argument("--env-file", type=Path, default=Path(".env"))
    configure_parser.add_argument("--force", action="store_true")
    doctor_parser = subparsers.add_parser("doctor")
    doctor_parser.add_argument("--env-file", type=Path, default=Path(".env"))
    subparsers.add_parser("validate")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the PermutiveAPI command-line interface."""
    args = build_parser().parse_args(argv)
    if args.command == "configure":
        return configure(args.env_file, force=args.force)
    if args.command == "doctor":
        return doctor(args.env_file)
    return validate()


if __name__ == "__main__":
    raise SystemExit(main())
