"""First-class Codex integration for PermutiveAPI."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from typing import Any, Mapping, cast

from ..actionable_errors import classify_exception
from ..agent import PermutiveAgentKit
from ..capabilities import (
    CapabilityDescriptor,
    CapabilityRequirement,
    descriptor_from_registry,
)
from ..client import PermutiveClient
from ..credentials import (
    CredentialsError,
    CredentialsProvider,
    LocalCredentialsProvider,
)
from ..mcp import PermutiveMCPConfig
from ..sdk import JSONObject
from ..tools import ToolDefinition, ToolRegistry
from .base import PLUGIN_API_VERSION, PluginMetadata
from .runtime import PluginMode, PluginPolicy, ValidationReport


def _sdk_version() -> str:
    try:
        return version("PermutiveAPI")
    except PackageNotFoundError:  # pragma: no cover
        return "0+unknown"


def _negotiation_plugin_api_version() -> str:
    return (
        PLUGIN_API_VERSION if "." in PLUGIN_API_VERSION else f"{PLUGIN_API_VERSION}.0"
    )


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
        return cls(
            policy=PluginPolicy(
                mode=cast(PluginMode, mode),
                allowed_tools=(
                    None if allowed_tools is None else frozenset(allowed_tools)
                ),
                require_confirmation_for_writes=require_confirmation_for_writes,
            ),
            mcp=mcp,
        )

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
            return PermutiveClient(credentials.load().api_key)
        if self._client is None:
            self._client = PermutiveClient(self._credentials.load().api_key)
        return self._client

    @staticmethod
    def _object_schema(*, required: tuple[str, ...] = ()) -> dict[str, Any]:
        properties: dict[str, Any] = {
            "resource_id": {"type": "string", "minLength": 1},
            "payload": {"type": "object"},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
        }
        schema: dict[str, Any] = {
            "type": "object",
            "properties": properties,
            "additionalProperties": False,
        }
        if required:
            schema["required"] = list(required)
        return schema

    def _definitions(self, client: PermutiveClient) -> tuple[ToolDefinition, ...]:
        def list_cohorts(page_size: int = 100) -> Any:
            return client.cohorts.list(page_size=page_size).items

        def get_cohort(resource_id: str) -> Any:
            return client.cohorts.get(resource_id)

        def list_segments(page_size: int = 100) -> Any:
            return client.segments.list(page_size=page_size).items

        def get_workspace(resource_id: str) -> Any:
            return client.workspaces.get(resource_id)

        def create_cohort(payload: JSONObject) -> Any:
            return client.cohorts.create(payload)

        def update_cohort(resource_id: str, payload: JSONObject) -> Any:
            return client.cohorts.update(resource_id, payload)

        return (
            ToolDefinition(
                "permutive_list_cohorts",
                "List Permutive cohorts with bounded pagination.",
                self._object_schema(),
                list_cohorts,
                ("cohorts", "read"),
            ),
            ToolDefinition(
                "permutive_get_cohort",
                "Get one Permutive cohort by identifier.",
                self._object_schema(required=("resource_id",)),
                get_cohort,
                ("cohorts", "read"),
            ),
            ToolDefinition(
                "permutive_list_segments",
                "List Permutive audience segments with bounded pagination.",
                self._object_schema(),
                list_segments,
                ("segments", "read"),
            ),
            ToolDefinition(
                "permutive_get_workspace",
                "Get one Permutive workspace by identifier.",
                self._object_schema(required=("resource_id",)),
                get_workspace,
                ("workspaces", "read"),
            ),
            ToolDefinition(
                "permutive_create_cohort",
                "Create a Permutive cohort from a JSON payload.",
                self._object_schema(required=("payload",)),
                create_cohort,
                ("cohorts", "write"),
                False,
            ),
            ToolDefinition(
                "permutive_update_cohort",
                "Update a Permutive cohort from a JSON payload.",
                self._object_schema(required=("resource_id", "payload")),
                update_cohort,
                ("cohorts", "write"),
                False,
            ),
        )

    def tools(self) -> ToolRegistry:
        """Return the curated tools allowed by the active policy."""
        if self._tools is None:
            definitions = self._definitions(self.create_client())
            self._tools = ToolRegistry(
                tuple(
                    item
                    for item in definitions
                    if self._policy.allows(item.name, read_only=item.read_only)
                )
            )
        return self._tools

    def capability_descriptor(self) -> CapabilityDescriptor:
        """Return versioned Codex plugin capability metadata."""
        interfaces = ["codex_plugin", "json_schema", "openai_tools", "tool_registry"]
        features = ["local_credentials", "read_write_policy"]
        if self._mcp is not None:
            interfaces.append("mcp_client_config")
            features.append("official_mcp")
        return descriptor_from_registry(
            self.tools(),
            surface="codex_plugin",
            interfaces=tuple(interfaces),
            features=tuple(features),
            plugin_api_version=_negotiation_plugin_api_version(),
        )

    def negotiate(
        self,
        requirement: CapabilityRequirement,
    ) -> CapabilityDescriptor:
        """Validate Codex requirements before tool invocation."""
        return self.capability_descriptor().negotiate(requirement)

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

    def invoke_safe(
        self,
        name: str,
        arguments: Mapping[str, Any] | None = None,
        *,
        confirmed: bool = False,
    ) -> dict[str, Any]:
        """Invoke a tool and return secret-safe actionable result metadata."""
        try:
            output = self.invoke(name, arguments, confirmed=confirmed)
        except Exception as exc:  # noqa: BLE001 - plugin boundary returns data
            guidance = classify_exception(exc, operation=name)
            return {
                "ok": False,
                "output": None,
                "error_type": type(exc).__name__,
                "error_code": guidance.code,
                "retryable": guidance.retryable,
                "recommended_action": guidance.recommended_action,
                "safe_context": dict(guidance.safe_context),
            }
        return {
            "ok": True,
            "output": output,
            "error_type": None,
            "error_code": None,
            "retryable": False,
            "recommended_action": None,
            "safe_context": {"operation": name},
        }

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
