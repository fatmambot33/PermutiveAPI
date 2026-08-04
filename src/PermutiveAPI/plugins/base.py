"""Stable plugin surface for PermutiveAPI integrations."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Dict, Iterable, Protocol

from ..client import PermutiveClient
from ..credentials import CredentialsProvider

PLUGIN_ENTRY_POINT_GROUP = "permutiveapi.plugins"


@dataclass(frozen=True)
class PluginMetadata:
    """Stable metadata exposed by a PermutiveAPI plugin."""

    name: str
    version: str
    description: str
    capabilities: tuple[str, ...]


class Plugin(Protocol):
    """Contract for first-class PermutiveAPI plugins."""

    @property
    def metadata(self) -> PluginMetadata:
        """Return immutable plugin metadata."""
        ...

    def create_client(self, credentials: CredentialsProvider) -> PermutiveClient:
        """Create an authenticated Permutive client."""
        ...


def discover_plugins() -> Dict[str, Plugin]:
    """Discover installed plugins through Python package entry points."""
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
        discovered[plugin.metadata.name] = plugin
    return discovered
