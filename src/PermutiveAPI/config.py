"""Typed configuration shared by SDK integration surfaces."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Optional, Tuple
from urllib.parse import urlparse

from .sdk import RetryPolicy


@dataclass(frozen=True)
class Secret:
    """Secret value whose representation is always redacted."""

    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("secret must not be empty")

    def __repr__(self) -> str:
        return "Secret('[REDACTED]')"

    def __str__(self) -> str:
        return "[REDACTED]"


@dataclass(frozen=True)
class PermutiveConfig:
    """Validated configuration for sync, async, CLI, MCP, and plugins."""

    api_key: Secret
    base_url: str = "https://api.permutive.com"
    timeout: Tuple[float, float] = (3.05, 30.0)
    retry_policy: RetryPolicy = RetryPolicy()
    allow_insecure_localhost: bool = False

    def __post_init__(self) -> None:
        parsed = urlparse(self.base_url)
        local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if parsed.scheme != "https" and not (local and self.allow_insecure_localhost):
            raise ValueError("base_url must use HTTPS unless localhost is explicitly allowed")
        if min(self.timeout) <= 0:
            raise ValueError("timeout values must be positive")

    @classmethod
    def from_env(
        cls,
        environ: Optional[Mapping[str, str]] = None,
        **overrides: object,
    ) -> "PermutiveConfig":
        """Load configuration with explicit values overriding environment values."""
        env = os.environ if environ is None else environ
        api_key = overrides.pop("api_key", None) or env.get("PERMUTIVE_API_KEY")
        if isinstance(api_key, Secret):
            secret = api_key
        elif isinstance(api_key, str):
            secret = Secret(api_key)
        else:
            raise ValueError("PERMUTIVE_API_KEY is required")
        base_url = overrides.pop("base_url", None) or env.get(
            "PERMUTIVE_BASE_URL", "https://api.permutive.com"
        )
        return cls(api_key=secret, base_url=str(base_url), **overrides)
