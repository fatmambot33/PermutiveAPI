"""Safe runtime policy and validation primitives for PermutiveAPI plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

PluginMode = Literal["read_only", "read_write"]


@dataclass(frozen=True)
class PluginPolicy:
    """Control which plugin operations may be exposed to an agent."""

    mode: PluginMode = "read_only"
    allowed_tools: frozenset[str] | None = None
    require_confirmation_for_writes: bool = True

    def allows(self, name: str, *, read_only: bool) -> bool:
        """Return whether a tool is allowed by this policy."""
        if self.allowed_tools is not None and name not in self.allowed_tools:
            return False
        return read_only or self.mode == "read_write"


@dataclass(frozen=True)
class ValidationReport:
    """Describe whether a plugin is ready for use."""

    valid: bool
    checks: tuple[str, ...]
    errors: tuple[str, ...] = ()


__all__ = ["PluginMode", "PluginPolicy", "ValidationReport"]
