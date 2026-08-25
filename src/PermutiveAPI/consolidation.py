"""Deterministic 6.8 consolidation evidence for public, resource, and integration contracts."""

from __future__ import annotations

import hashlib
import json
from typing import Iterable

from .capabilities import capability_contract_manifest
from .integration_registry import integration_registry_manifest
from .public_surface import public_surface_manifest
from .resource_registry import resource_registry_manifest

CONSOLIDATION_EVIDENCE_VERSION = 1


def consolidation_manifest(exports: Iterable[str]) -> dict[str, object]:
    """Return reproducible first-class SDK consolidation evidence."""
    return {
        "version": CONSOLIDATION_EVIDENCE_VERSION,
        "public_surface": public_surface_manifest(exports),
        "resources": resource_registry_manifest(),
        "integrations": integration_registry_manifest(),
        "capabilities": capability_contract_manifest(),
    }


def consolidation_json(exports: Iterable[str]) -> str:
    """Serialize consolidation evidence deterministically."""
    return json.dumps(
        consolidation_manifest(exports),
        sort_keys=True,
        separators=(",", ":"),
    )


def consolidation_fingerprint(exports: Iterable[str]) -> str:
    """Return a stable SHA-256 fingerprint for consolidation evidence."""
    return hashlib.sha256(consolidation_json(exports).encode("utf-8")).hexdigest()


__all__ = [
    "CONSOLIDATION_EVIDENCE_VERSION",
    "consolidation_fingerprint",
    "consolidation_json",
    "consolidation_manifest",
]
