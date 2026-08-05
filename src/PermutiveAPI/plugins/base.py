"""Stable plugin surface for PermutiveAPI integrations."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Dict, Iterable, Protocol

from ..agent import PermutiveAgentKit
from ..client import PermutiveClient
from ..credentials import CredentialsProvider
from ..tools import ToolRegistry
from .runtime import ValidationReport

PLUGIN_ENTRY_POINT_GROUP = "permutiveapi.plugins"
PLUGIN_API_VERSION = "1"


@dataclass(frozen=True)
class PluginMetadata:
    """Stable metadata exposed by a PermutiveAPI plugin."""

    name: str
    plugin_version: str
    sdk_version: str
    api_version: str
    description: str
    capabilities: tuple[str, ...]


class Plugin(Protocol):
    """Contract for first-class PermutiveAPI plugins."""

    @property
    def metadata(self) -> PluginMetadata:
        """Return immutable plugin metadata."""
        ...

    def create_client(
        self, credentials: CredentialsProvider | None = None
    ) -> PermutiveClient:
        """Create an authenticated Permutive client."""
        ...

    def tools(self) -> ToolRegistry:
        """Return tools allowed by the active plugin policy."""
        ...

    def agent_kit(self) -> PermutiveAgentKit:
        """Return a complete agent integration bundle."""
        ...

    def validate(self) -> ValidationReport:
        """Validate plugin configuration without exposing secrets."""
        ...

    def close(self) -> None:
        """Release resources owned by the plugin."""
        ...


def discover_plugins() -> Dict[str, Plugin]:
    """Discover and validate installed plugins through package entry points."""
    discovered: Dict[str, Plugin] = {}
    selected = entry_points()
    candidates: Iterable[object]
    if hasattr(selected, "select"):
        candidates = selected.select(group=PLUGIN_ENTRY_POINT_GROUP)
    else:  # pragma: no cover - Python 3.9 compatibility path
        candidates = selected.get(PLUGIN_ENTRY_POINT_GROUP, ())

    for candidate in candidates:
        plugin_type = candidate.load()
        plugin = plugin_type()
        metadata = plugin.metadata
        if metadata.api_version != PLUGIN_API_VERSION:
            raise RuntimeError(
                f"Unsupported plugin API version for {metadata.name}: "
                f"{metadata.api_version}"
            )
        if metadata.name in discovered:
            raise RuntimeError(f"Duplicate plugin name: {metadata.name}")
        discovered[metadata.name] = plugin
    return discovered


__all__ = [
    "PLUGIN_API_VERSION",
    "PLUGIN_ENTRY_POINT_GROUP",
    "Plugin",
    "PluginMetadata",
    "discover_plugins",
]
