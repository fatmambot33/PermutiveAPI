"""First-class Codex integration for PermutiveAPI."""

from __future__ import annotations

from typing import Optional

from ..client import PermutiveClient
from ..credentials import CredentialsProvider, LocalCredentialsProvider
from .base import PluginMetadata


class CodexPlugin:
    """Codex-facing plugin with secure local credential resolution."""

    @property
    def metadata(self) -> PluginMetadata:
        """Return the Codex plugin contract."""
        return PluginMetadata(
            name="codex",
            version="1",
            description="Authenticated PermutiveAPI surface for Codex agents.",
            capabilities=("client", "local-credentials", "resource-api"),
        )

    def create_client(
        self, credentials: Optional[CredentialsProvider] = None
    ) -> PermutiveClient:
        """Create a client using the supplied or default local provider."""
        provider = credentials or LocalCredentialsProvider()
        resolved = provider.load()
        return PermutiveClient(resolved.api_key)


def create_client(
    credentials: Optional[CredentialsProvider] = None,
) -> PermutiveClient:
    """Create a Codex-ready client with one import and sensible defaults."""
    return CodexPlugin().create_client(credentials)
