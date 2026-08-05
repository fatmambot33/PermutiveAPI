"""First-class Codex integration for PermutiveAPI."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any, Mapping

from ..agent import PermutiveAgentKit
from ..client import PermutiveClient
from ..credentials import CredentialsError, CredentialsProvider, LocalCredentialsProvider
from ..mcp import PermutiveMCPConfig
from ..tools import ToolDefinition, ToolRegistry
from .base import PLUGIN_API_VERSION, PluginMetadata
from .runtime import PluginPolicy, ValidationReport


def _sdk_version() -> str:
    try:
        return version("PermutiveAPI")
    except PackageNotFoundError:  # pragma: no cover - editable source tree
        return "0+unknown"


class CodexPlugin:
    """Cohesive, safe Codex-facing PermutiveAPI integration."""

    def __init__(
        self,
        credentials: CredentialsProvider | None = None,
        *,
        policy: PluginPolicy | None = None,
        mcp: PermutiveMCPConfig | None = None,
    ) -> None:
        self._credentials = credentials or LocalCredentialsProvider()
        self._policy = policy or PluginPolicy()
        self._mcp = mcp
        self._client: PermutiveClient | None = None
        self._tools: ToolRegistry | None = None

    @classmethod
    def from_env(
        cls,
        *,
        mode: str = "read_only",
        allowed_tools: set[str] | None = None,
        require_confirmation_for_writes: bool = True,
        mcp: PermutiveMCPConfig | None = None,
    ) -> "CodexPlugin":
        """Create a plugin using secure local credential resolution."""
        if mode not in {"read_only", "read_write"}:
            raise ValueError("mode must be 'read_only' or 'read_write'")
        policy = PluginPolicy(
            mode=mode,  # type: ignore[arg-type]
            allowed_tools=None if allowed_tools is None else frozenset(allowed_tools),
            require_confirmation_for_writes=require_confirmation_for_writes,
        )
        return cls(policy=policy, mcp=mcp)

    @property
    def metadata(self) -> PluginMetadata:
        """Return the stable Codex plugin contract."""
        return PluginMetadata(
            name="codex",
            plugin_version="1.0",
            sdk_version=_sdk_version(),
            api_version=PLUGIN_API_VERSION,
            description="Safe PermutiveAPI tools and MCP configuration for agents.",
            capabilities=(
                "client",
                "agent-tools",
                "local-credentials",
                "official-mcp",
                "read-write-policy",
            ),
        )

    def create_client(
        self, credentials: CredentialsProvider | None = None
    ) -> PermutiveClient:
        """Create or return an authenticated Permutive client."""
        if credentials is not None:
            resolved = credentials.load()
            return PermutiveClient(resolved.api_key)
        if self._client is None:
            resolved = self._credentials.load()
            self._client = PermutiveClient(resolved.api_key)
        return self._client

    def tools(self) -> ToolRegistry:
        """Return the curated tools allowed by the active policy."""
        if self._tools is None:
            client = self.create_client()
            definitions = (
                ToolDefinition(
                    name="permutive_list_cohorts",
                    description="List Permutive cohorts with bounded pagination.",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "page_size": {"type": "integer", "minimum": 1, "maximum": 100}
                        },
                        "additionalProperties": False,
                    },
                    handler=lambda page_size=100: client.list_page(
                        "cohorts", page_size=page_size
                    ).items,
                    tags=("cohorts", "read"),
                ),
                ToolDefinition(
                    name="permutive_get_cohort",
                    description="Get one Permutive cohort by identifier.",
                    input_schema={
                        "type": "object",
                        "properties": {"cohort_id": {"type": "string", "minLength": 1}},
                        "required": ["cohort_id"],
                        "additionalProperties": False,
                    },
                    handler=lambda cohort_id: client.request("GET", f"cohorts/{cohort_id}"),
                    tags=("cohorts", "read"),
                ),
                ToolDefinition(
                    name="permutive_list_segments",
                    description="List Permutive audience segments with bounded pagination.",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "page_size": {"type": "integer", "minimum": 1, "maximum": 100}
                        },
                        "additionalProperties": False,
                    },
                    handler=lambda page_size=100: client.list_page(
                        "segments", page_size=page_size
                    ).items,
                    tags=("segments", "read"),
                ),
                ToolDefinition(
                    name="permutive_get_workspace",
                    description="Get one Permutive workspace by identifier.",
                    input_schema={
                        "type": "object",
                        "properties": {"workspace_id": {"type": "string", "minLength": 1}},
                        "required": ["workspace_id"],
                        "additionalProperties": False,
                    },
                    handler=lambda workspace_id: client.request(
                        "GET", f"workspaces/{workspace_id}"
                    ),
                    tags=("workspaces", "read"),
                ),
                ToolDefinition(
                    name="permutive_create_cohort",
                    description="Create a Permutive cohort from a validated JSON payload.",
                    input_schema={
                        "type": "object",
                        "properties": {"payload": {"type": "object"}},
                        "required": ["payload"],
                        "additionalProperties": False,
                    },
                    handler=lambda payload: client.request("POST", "cohorts", json=payload),
                    tags=("cohorts", "write"),
                    read_only=False,
                ),
                ToolDefinition(
                    name="permutive_update_cohort",
                    description="Update a Permutive cohort from a validated JSON payload.",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "cohort_id": {"type": "string", "minLength": 1},
                            "payload": {"type": "object"},
                        },
                        "required": ["cohort_id", "payload"],
                        "additionalProperties": False,
                    },
                    handler=lambda cohort_id, payload: client.request(
                        "PATCH", f"cohorts/{cohort_id}", json=payload
                    ),
                    tags=("cohorts", "write"),
                    read_only=False,
                ),
            )
            self._tools = ToolRegistry(
                tuple(
                    definition
                    for definition in definitions
                    if self._policy.allows(
                        definition.name, read_only=definition.read_only
                    )
                )
            )
        return self._tools

    def agent_kit(self) -> PermutiveAgentKit:
        """Return one complete agent integration bundle."""
        return PermutiveAgentKit(self.tools(), mcp=self._mcp)

    def invoke(
        self,
        name: str,
        arguments: Mapping[str, Any] | None = None,
        *,
        confirmed: bool = False,
    ) -> Any:
        """Invoke a tool while enforcing write confirmation policy."""
        definition = self.tools().get(name)
        if (
            not definition.read_only
            and self._policy.require_confirmation_for_writes
            and not confirmed
        ):
            raise PermissionError(f"Write tool requires explicit confirmation: {name}")
        return definition.invoke(arguments)

    def validate(self) -> ValidationReport:
        """Validate credentials and policy without making an API request."""
        checks = ["plugin-api", "policy"]
        errors: list[str] = []
        try:
            self._credentials.load()
            checks.append("credentials")
        except CredentialsError as exc:
            errors.append(str(exc))
        if self._mcp is not None:
            checks.append("official-mcp")
        return ValidationReport(not errors, tuple(checks), tuple(errors))

    def diagnostics(self) -> dict[str, Any]:
        """Return a secret-safe runtime summary."""
        report = self.validate()
        return {
            "metadata": self.metadata,
            "policy": {
                "mode": self._policy.mode,
                "write_confirmation": self._policy.require_confirmation_for_writes,
            },
            "tools": self.tools().capabilities() if report.valid else None,
            "validation": report,
        }

    def close(self) -> None:
        """Close the owned client and clear cached runtime state."""
        if self._client is not None:
            self._client.close()
        self._client = None
        self._tools = None


def create_client(
    credentials: CredentialsProvider | None = None,
) -> PermutiveClient:
    """Create a Codex-ready client with one import and sensible defaults."""
    return CodexPlugin(credentials).create_client()


__all__ = ["CodexPlugin", "create_client"]
