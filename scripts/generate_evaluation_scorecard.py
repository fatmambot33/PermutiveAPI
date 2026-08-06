"""Generate or validate the deterministic AI evaluation scorecard."""

from __future__ import annotations

import argparse
from pathlib import Path

from PermutiveAPI.evaluations import run_default_evaluations


def main() -> int:
    """Generate the canonical scorecard or compare it with committed evidence."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    scorecard = run_default_evaluations()
    content = scorecard.to_json()
    if args.check is not None:
        if not args.check.is_file():
            print(f"Evaluation scorecard is missing: {args.check}")
            return 1
        if args.check.read_text(encoding="utf-8") != content:
            print(f"Evaluation scorecard is stale: {args.check}")
            return 1
        if not scorecard.ok:
            print("Evaluation scorecard contains failures.")
            return 1
        print(
            "Evaluation scorecard passed "
            f"{scorecard.passed}/{scorecard.total} deterministic cases."
        )
        return 0

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content, encoding="utf-8")
    else:
        print(content, end="")
    return 0 if scorecard.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
