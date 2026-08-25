"""Generated classification for the package-root public SDK surface."""

from __future__ import annotations

from typing import Iterable, Tuple

PUBLIC_SURFACE_VERSION = 1

_COMPATIBILITY_EXPORTS = frozenset(
    {
        "Alias",
        "Cohort",
        "CohortList",
        "ContextSegment",
        "Event",
        "Identity",
        "Import",
        "ImportList",
        "Segment",
        "SegmentList",
        "Segmentation",
        "Source",
        "Workspace",
        "WorkspaceList",
    }
)

_INTEGRATION_EXPORTS = frozenset(
    {
        "CapabilityDescriptor",
        "CapabilityNegotiationError",
        "CapabilityRequirement",
        "JSONSchema",
        "PERMUTIVE_MCP_DOCUMENTATION_URL",
        "PERMUTIVE_MCP_SERVER_NAME",
        "PERMUTIVE_MCP_TOKEN_ENV",
        "PERMUTIVE_MCP_URL_ENV",
        "PermutiveAgentKit",
        "PermutiveMCPConfig",
        "ToolDefinition",
        "ToolHandler",
        "ToolRegistry",
        "capability_contract_manifest",
        "negotiate_capabilities",
        "tool",
    }
)


def classify_public_export(name: str) -> str:
    """Return the stable classification for one package-root export."""
    if name in _COMPATIBILITY_EXPORTS:
        return "compatibility"
    if name in _INTEGRATION_EXPORTS:
        return "integration"
    return "canonical"


def public_surface_manifest(exports: Iterable[str]) -> dict[str, object]:
    """Build deterministic package-root public API evidence."""
    values: Tuple[str, ...] = tuple(exports)
    if len(values) != len(set(values)):
        raise ValueError("Package-root exports must not contain duplicates.")
    classified = {
        category: sorted(
            name for name in values if classify_public_export(name) == category
        )
        for category in ("canonical", "integration", "compatibility")
    }
    return {
        "version": PUBLIC_SURFACE_VERSION,
        "canonical_import": "PermutiveAPI",
        "export_count": len(values),
        "classifications": classified,
    }


__all__ = [
    "PUBLIC_SURFACE_VERSION",
    "classify_public_export",
    "public_surface_manifest",
]
