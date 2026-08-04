"""Typed configuration helpers for the official Permutive MCP server.

The MCP server is hosted and versioned by Permutive. This module intentionally
provides client configuration only; it does not proxy or duplicate hosted MCP
tools.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlparse

PERMUTIVE_MCP_URL_ENV = "PERMUTIVE_MCP_URL"
PERMUTIVE_MCP_TOKEN_ENV = "PERMUTIVE_MCP_TOKEN"
PERMUTIVE_MCP_SERVER_NAME = "permutive"
PERMUTIVE_MCP_DOCUMENTATION_URL = "https://docs.permutive.com/api/mcp/introduction"


@dataclass(frozen=True)
class PermutiveMCPConfig:
    """Describe a connection to the official Permutive MCP server.

    Parameters
    ----------
    url
        HTTPS endpoint supplied by Permutive for the MCP server.
    token
        Optional bearer token supplied by Permutive. It is excluded from repr.
    server_name
        Name used in generated MCP client configuration.
    headers
        Additional HTTP headers merged with bearer authentication.
    """

    url: str
    token: str | None = field(default=None, repr=False)
    server_name: str = PERMUTIVE_MCP_SERVER_NAME
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        """Validate and normalize configuration values."""
        url = self.url.strip()
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("Permutive MCP URL must be an absolute HTTPS URL.")
        if parsed.username or parsed.password:
            raise ValueError("Permutive MCP URL must not contain credentials.")

        server_name = self.server_name.strip()
        if not server_name:
            raise ValueError("MCP server name must not be empty.")

        token = self.token.strip() if self.token is not None else None
        if token == "":
            token = None

        normalized_headers: dict[str, str] = {}
        for key, value in self.headers.items():
            header_name = key.strip()
            header_value = value.strip()
            if not header_name or not header_value:
                raise ValueError("MCP header names and values must not be empty.")
            normalized_headers[header_name] = header_value

        object.__setattr__(self, "url", url)
        object.__setattr__(self, "token", token)
        object.__setattr__(self, "server_name", server_name)
        object.__setattr__(self, "headers", MappingProxyType(normalized_headers))

    @classmethod
    def from_env(
        cls,
        *,
        url_variable: str = PERMUTIVE_MCP_URL_ENV,
        token_variable: str = PERMUTIVE_MCP_TOKEN_ENV,
        server_name: str = PERMUTIVE_MCP_SERVER_NAME,
    ) -> "PermutiveMCPConfig":
        """Create a validated configuration from environment variables.

        Parameters
        ----------
        url_variable
            Environment variable containing the MCP endpoint.
        token_variable
            Environment variable containing an optional bearer token.
        server_name
            Name used in generated MCP client configuration.

        Returns
        -------
        PermutiveMCPConfig
            Validated MCP configuration.

        Raises
        ------
        ValueError
            If the endpoint variable is missing or invalid.
        """
        url = os.getenv(url_variable)
        if url is None or not url.strip():
            raise ValueError(f"Missing required environment variable: {url_variable}")
        return cls(url=url, token=os.getenv(token_variable), server_name=server_name)

    def resolved_headers(self) -> dict[str, str]:
        """Return headers ready for an HTTP MCP client.

        Returns
        -------
        dict[str, str]
            A new dictionary containing configured headers and authentication.
        """
        headers = dict(self.headers)
        if self.token is not None:
            existing = next(
                (key for key in headers if key.lower() == "authorization"),
                None,
            )
            if existing is not None:
                raise ValueError("Set either token or Authorization header, not both.")
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def to_client_config(self) -> dict[str, object]:
        """Return a portable HTTP MCP client configuration fragment.

        Returns
        -------
        dict[str, object]
            Configuration using the common ``mcpServers`` shape.
        """
        server: dict[str, object] = {"type": "http", "url": self.url}
        headers = self.resolved_headers()
        if headers:
            server["headers"] = headers
        return {"mcpServers": {self.server_name: server}}

    def to_json(self, *, indent: int = 2) -> str:
        """Serialize the portable client configuration as JSON.

        Parameters
        ----------
        indent
            Number of spaces used for indentation.

        Returns
        -------
        str
            Deterministically formatted JSON configuration.
        """
        if indent < 0:
            raise ValueError("JSON indentation must be zero or greater.")
        return json.dumps(self.to_client_config(), indent=indent, sort_keys=True)


__all__ = [
    "PERMUTIVE_MCP_DOCUMENTATION_URL",
    "PERMUTIVE_MCP_SERVER_NAME",
    "PERMUTIVE_MCP_TOKEN_ENV",
    "PERMUTIVE_MCP_URL_ENV",
    "PermutiveMCPConfig",
]
