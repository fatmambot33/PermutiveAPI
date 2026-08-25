"""Shared typed metadata for agent, plugin, MCP, and tool integration surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

INTEGRATION_REGISTRY_VERSION = 1


@dataclass(frozen=True)
class IntegrationSurface:
    """Describe one supported extension surface without framework coupling."""

    name: str
    interface: str
    features: Tuple[str, ...]
    governed: bool = True

    def __post_init__(self) -> None:
        """Normalize deterministic integration metadata."""
        if not self.name or not self.interface:
            raise ValueError("Integration names and interfaces must not be empty.")
        object.__setattr__(self, "features", tuple(sorted(set(self.features))))

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable integration metadata."""
        return {
            "name": self.name,
            "interface": self.interface,
            "features": list(self.features),
            "governed": self.governed,
        }


_SURFACES = (
    IntegrationSurface(
        "tools",
        "ToolRegistry",
        ("discovery", "typed_schema", "invocation"),
    ),
    IntegrationSurface(
        "agent",
        "PermutiveAgentKit",
        ("capability_discovery", "governed_invocation", "tool_registry"),
    ),
    IntegrationSurface(
        "plugin",
        "CodexPlugin",
        ("discovery", "governed_invocation", "safe_results"),
    ),
    IntegrationSurface(
        "mcp",
        "PermutiveMCPConfig",
        ("capability_discovery", "environment_configuration", "remote_composition"),
    ),
)


def integration_surfaces() -> Tuple[IntegrationSurface, ...]:
    """Return supported integration surfaces in deterministic order."""
    return tuple(sorted(_SURFACES, key=lambda surface: surface.name))


def integration_surface(name: str) -> IntegrationSurface:
    """Return one supported integration surface by stable name."""
    for surface in integration_surfaces():
        if surface.name == name:
            return surface
    raise KeyError(f"Unknown integration surface: {name}")


def integration_registry_manifest() -> dict[str, object]:
    """Return deterministic shared integration capability metadata."""
    return {
        "version": INTEGRATION_REGISTRY_VERSION,
        "surfaces": [surface.to_dict() for surface in integration_surfaces()],
    }


__all__ = [
    "INTEGRATION_REGISTRY_VERSION",
    "IntegrationSurface",
    "integration_registry_manifest",
    "integration_surface",
    "integration_surfaces",
]
