"""Immutable release-candidate manifest creation and verification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

RELEASE_EVIDENCE_VERSION = 1


@dataclass(frozen=True)
class ArtifactDigest:
    """Describe one immutable release artifact."""

    path: str
    size: int
    sha256: str

    def to_dict(self) -> dict[str, object]:
        """Return deterministic artifact evidence."""
        return {"path": self.path, "size": self.size, "sha256": self.sha256}


def digest_file(path: Path, *, root: Path) -> ArtifactDigest:
    """Calculate one artifact digest relative to an evidence root."""
    resolved_root = root.resolve()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"Artifact is outside the evidence root: {path}") from exc
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return ArtifactDigest(relative.as_posix(), resolved.stat().st_size, digest.hexdigest())


def create_release_manifest(
    *,
    project: str,
    version: str,
    source_commit: str,
    files: Iterable[Path],
    root: Path,
) -> dict[str, object]:
    """Build deterministic evidence for an exact release candidate file set."""
    if not project or not version or not source_commit:
        raise ValueError("Project, version, and source commit are required.")
    artifacts = tuple(
        sorted(
            (digest_file(path, root=root) for path in files),
            key=lambda item: item.path,
        )
    )
    if not artifacts:
        raise ValueError("A release candidate must contain at least one artifact.")
    return {
        "version": RELEASE_EVIDENCE_VERSION,
        "project": project,
        "package_version": version,
        "source_commit": source_commit,
        "artifacts": [artifact.to_dict() for artifact in artifacts],
    }


def write_release_manifest(manifest: Mapping[str, object], path: Path) -> None:
    """Write deterministic UTF-8 release evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(manifest), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def verify_release_manifest(path: Path, *, root: Path) -> dict[str, object]:
    """Verify every recorded file hash and size and return decoded evidence."""
    decoded: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError("Release evidence must be a JSON object.")
    if decoded.get("version") != RELEASE_EVIDENCE_VERSION:
        raise ValueError("Unsupported release evidence version.")
    artifacts = decoded.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("Release evidence must contain artifacts.")
    seen = set()
    for raw in artifacts:
        if not isinstance(raw, dict):
            raise TypeError("Release artifact evidence must be an object.")
        relative = raw.get("path")
        expected_size = raw.get("size")
        expected_digest = raw.get("sha256")
        if not isinstance(relative, str) or relative in seen:
            raise ValueError("Release artifact paths must be unique strings.")
        if not isinstance(expected_size, int) or isinstance(expected_size, bool):
            raise TypeError("Release artifact size must be an integer.")
        if not isinstance(expected_digest, str):
            raise TypeError("Release artifact digest must be a string.")
        seen.add(relative)
        actual = digest_file(root / relative, root=root)
        if actual.size != expected_size or actual.sha256 != expected_digest:
            raise ValueError(f"Release artifact verification failed: {relative}")
    return decoded


__all__ = [
    "RELEASE_EVIDENCE_VERSION",
    "ArtifactDigest",
    "create_release_manifest",
    "digest_file",
    "verify_release_manifest",
    "write_release_manifest",
]
