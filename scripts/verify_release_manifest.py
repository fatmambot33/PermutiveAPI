"""Verify immutable release-candidate evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

from PermutiveAPI.release_evidence import verify_release_manifest


def main() -> int:
    """Verify every file recorded in a release candidate manifest."""
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()

    manifest = verify_release_manifest(args.manifest, root=args.root)
    artifacts = manifest["artifacts"]
    if not isinstance(artifacts, list):
        raise TypeError("Release manifest artifacts must be a list.")
    print(f"Release candidate verification passed for {len(artifacts)} artifacts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
