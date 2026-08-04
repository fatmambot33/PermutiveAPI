"""Credential providers for local and plugin-driven SDK use."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional, Protocol, Sequence

from dotenv import dotenv_values


class CredentialsError(RuntimeError):
    """Raised when credentials cannot be resolved safely."""


@dataclass(frozen=True)
class Credentials:
    """Resolved Permutive credentials.

    Parameters
    ----------
    api_key
        Permutive API key.
    source
        Human-readable source identifier that never contains the secret.
    """

    api_key: str
    source: str

    def __repr__(self) -> str:
        """Return a representation that never exposes the API key."""
        return f"Credentials(api_key='[REDACTED]', source={self.source!r})"


class CredentialsProvider(Protocol):
    """Contract implemented by credential provider plugins."""

    def load(self) -> Credentials:
        """Resolve credentials or raise ``CredentialsError``."""
        ...


class LocalCredentialsProvider:
    """Resolve credentials from explicit input, environment, or local files.

    Resolution order is deterministic:

    1. explicit ``api_key``
    2. process environment
    3. configured dotenv files, in order

    The provider never mutates ``os.environ`` and never returns a blank key.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        env_var: str = "PERMUTIVE_API_KEY",
        dotenv_paths: Optional[Sequence[Path | str]] = None,
        environ: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._api_key = api_key
        self._env_var = env_var
        self._dotenv_paths = tuple(
            Path(path).expanduser()
            for path in (
                dotenv_paths
                if dotenv_paths is not None
                else (Path.cwd() / ".env", Path.home() / ".config/permutive/.env")
            )
        )
        self._environ = environ if environ is not None else os.environ

    def load(self) -> Credentials:
        """Resolve the first valid local credential."""
        explicit = self._normalize(self._api_key)
        if explicit is not None:
            return Credentials(explicit, "explicit")

        environment = self._normalize(self._environ.get(self._env_var))
        if environment is not None:
            return Credentials(environment, f"environment:{self._env_var}")

        for path in self._dotenv_paths:
            if not path.is_file():
                continue
            value = self._normalize(dotenv_values(path).get(self._env_var))
            if value is not None:
                return Credentials(value, f"dotenv:{path}")

        searched = ", ".join(str(path) for path in self._dotenv_paths)
        raise CredentialsError(
            f"No Permutive API key found in explicit input, {self._env_var}, "
            f"or dotenv files: {searched}"
        )

    @staticmethod
    def _normalize(value: object) -> Optional[str]:
        """Return a stripped non-empty string value."""
        if not isinstance(value, str):
            return None
        normalized = value.strip()
        return normalized or None
