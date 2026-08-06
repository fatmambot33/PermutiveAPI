"""Strict downstream type-consumption smoke example."""

from __future__ import annotations

from PermutiveAPI import (
    AsyncPermutiveClient,
    PermutiveClient,
    QueryExpression,
    all_of,
    event,
)
from PermutiveAPI.plugins.codex import CodexPlugin


def create_sync_client(api_key: str) -> PermutiveClient:
    """Create the canonical synchronous client."""
    return PermutiveClient(api_key)


def create_async_client(api_key: str) -> AsyncPermutiveClient:
    """Create the canonical asynchronous client."""
    return AsyncPermutiveClient(api_key)


def create_query() -> QueryExpression:
    """Create one typed query expression."""
    return all_of(event("Pageview"))


def preserve_plugin_type(plugin: CodexPlugin) -> CodexPlugin:
    """Confirm that the documented plugin type is consumable downstream."""
    return plugin
