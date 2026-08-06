"""Build immutable release-candidate evidence for exact artifact files."""

from __future__ import annotations

import argparse
from pathlib import Path

from PermutiveAPI.release_evidence import (
    create_release_manifest,
    write_release_manifest,
)


def main() -> int:
    """Create one deterministic release candidate manifest."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    manifest = create_release_manifest(
        project=args.project,
        version=args.version,
        source_commit=args.source_commit,
        files=args.files,
        root=args.root,
    )
    write_release_manifest(manifest, args.output)
    print(f"Release manifest contains {len(args.files)} immutable artifacts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
