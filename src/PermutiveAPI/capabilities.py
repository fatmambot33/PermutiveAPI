"""Versioned capability discovery and negotiation contracts."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Mapping, Optional, Protocol, Tuple

CAPABILITY_CONTRACT_VERSION = "1.0"
TOOL_SCHEMA_VERSION = "1.0"
CAPABILITY_ERROR_CODES = (
    "capability_contract_incompatible",
    "tool_schema_incompatible",
    "plugin_api_incompatible",
    "capability_missing",
)


class RegistryCapabilities(Protocol):
    """Registry metadata required to build a capability descriptor."""

    def capabilities(self) -> Mapping[str, object]:
        """Return deterministic registry capability metadata."""
        ...


@dataclass(frozen=True)
class CapabilityRequirement:
    """Describe capabilities required by one integration consumer."""

    contract_version: str = CAPABILITY_CONTRACT_VERSION
    tool_schema_version: str = TOOL_SCHEMA_VERSION
    plugin_api_version: Optional[str] = None
    interfaces: Tuple[str, ...] = ()
    features: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Normalize and validate requirement values."""
        _parse_version(self.contract_version)
        _parse_version(self.tool_schema_version)
        if self.plugin_api_version is not None:
            _parse_version(self.plugin_api_version)
        object.__setattr__(self, "interfaces", _normalized(self.interfaces))
        object.__setattr__(self, "features", _normalized(self.features))

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-compatible requirement."""
        return {
            "contract_version": self.contract_version,
            "tool_schema_version": self.tool_schema_version,
            "plugin_api_version": self.plugin_api_version,
            "interfaces": list(self.interfaces),
            "features": list(self.features),
        }


@dataclass(frozen=True)
class CapabilityDescriptor:
    """Describe one versioned integration surface."""

    surface: str
    sdk_version: str
    interfaces: Tuple[str, ...]
    features: Tuple[str, ...]
    contract_version: str = CAPABILITY_CONTRACT_VERSION
    tool_schema_version: str = TOOL_SCHEMA_VERSION
    plugin_api_version: Optional[str] = None
    tool_count: int = 0
    read_only_tools: int = 0
    write_tools: int = 0

    def __post_init__(self) -> None:
        """Normalize and validate descriptor values."""
        if not self.surface:
            raise ValueError("surface must not be empty.")
        _parse_version(self.contract_version)
        _parse_version(self.tool_schema_version)
        if self.plugin_api_version is not None:
            _parse_version(self.plugin_api_version)
        if min(self.tool_count, self.read_only_tools, self.write_tools) < 0:
            raise ValueError("tool counts must be non-negative.")
        if self.read_only_tools + self.write_tools != self.tool_count:
            raise ValueError("read-only and write counts must equal tool_count.")
        object.__setattr__(self, "interfaces", _normalized(self.interfaces))
        object.__setattr__(self, "features", _normalized(self.features))

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable capability metadata."""
        return {
            "surface": self.surface,
            "contract_version": self.contract_version,
            "sdk_version": self.sdk_version,
            "tool_schema_version": self.tool_schema_version,
            "plugin_api_version": self.plugin_api_version,
            "interfaces": list(self.interfaces),
            "features": list(self.features),
            "tool_count": self.tool_count,
            "read_only_tools": self.read_only_tools,
            "write_tools": self.write_tools,
        }

    def negotiate(self, requirement: CapabilityRequirement) -> "CapabilityDescriptor":
        """Validate a consumer requirement and return this descriptor."""
        return negotiate_capabilities(self, requirement)


class CapabilityNegotiationError(ValueError):
    """Report one stable, actionable capability incompatibility."""

    def __init__(
        self,
        code: str,
        *,
        detail: str,
        recommended_action: str,
        missing: Tuple[str, ...] = (),
    ) -> None:
        if code not in CAPABILITY_ERROR_CODES:
            raise ValueError(f"Unknown capability negotiation code: {code}")
        self.code = code
        self.detail = detail
        self.recommended_action = recommended_action
        self.missing = _normalized(missing)
        super().__init__(f"{code}: {detail}")

    def to_dict(self) -> dict[str, object]:
        """Return safe structured failure metadata."""
        return {
            "code": self.code,
            "detail": self.detail,
            "recommended_action": self.recommended_action,
            "missing": list(self.missing),
        }


def installed_sdk_version() -> str:
    """Return installed package metadata without importing package internals."""
    try:
        return version("PermutiveAPI")
    except PackageNotFoundError:
        return "0+unknown"


def descriptor_from_registry(
    registry: RegistryCapabilities,
    *,
    surface: str,
    interfaces: Tuple[str, ...],
    features: Tuple[str, ...] = (),
    plugin_api_version: Optional[str] = None,
) -> CapabilityDescriptor:
    """Build a descriptor from one framework-neutral tool registry."""
    metadata = registry.capabilities()
    tool_count = _integer(metadata, "tool_count")
    read_only = _integer(metadata, "read_only_tools")
    write_count = _integer(metadata, "write_tools")
    derived = list(features)
    if tool_count:
        derived.extend(("tool_discovery", "tool_invocation"))
    if read_only:
        derived.append("read_tools")
    if write_count:
        derived.append("write_tools")
    return CapabilityDescriptor(
        surface=surface,
        sdk_version=installed_sdk_version(),
        interfaces=interfaces,
        features=tuple(derived),
        plugin_api_version=plugin_api_version,
        tool_count=tool_count,
        read_only_tools=read_only,
        write_tools=write_count,
    )


def negotiate_capabilities(
    descriptor: CapabilityDescriptor,
    requirement: CapabilityRequirement,
) -> CapabilityDescriptor:
    """Validate version and feature compatibility before execution."""
    if not _compatible_version(
        descriptor.contract_version,
        requirement.contract_version,
    ):
        raise CapabilityNegotiationError(
            "capability_contract_incompatible",
            detail=(
                f"Required contract {requirement.contract_version}; "
                f"available {descriptor.contract_version}."
            ),
            recommended_action=(
                "Use a consumer with the same contract major version and no newer minor."
            ),
        )
    if not _compatible_version(
        descriptor.tool_schema_version,
        requirement.tool_schema_version,
    ):
        raise CapabilityNegotiationError(
            "tool_schema_incompatible",
            detail=(
                f"Required tool schema {requirement.tool_schema_version}; "
                f"available {descriptor.tool_schema_version}."
            ),
            recommended_action="Regenerate tool schemas for the available contract.",
        )
    if requirement.plugin_api_version is not None:
        if descriptor.plugin_api_version is None or not _compatible_version(
            descriptor.plugin_api_version,
            requirement.plugin_api_version,
        ):
            raise CapabilityNegotiationError(
                "plugin_api_incompatible",
                detail=(
                    f"Required plugin API {requirement.plugin_api_version}; "
                    f"available {descriptor.plugin_api_version or 'none'}."
                ),
                recommended_action="Install a compatible plugin or relax the requirement.",
            )
    missing_interfaces = tuple(
        value for value in requirement.interfaces if value not in descriptor.interfaces
    )
    missing_features = tuple(
        value for value in requirement.features if value not in descriptor.features
    )
    missing = _normalized((*missing_interfaces, *missing_features))
    if missing:
        raise CapabilityNegotiationError(
            "capability_missing",
            detail="Required interfaces or features are unavailable.",
            recommended_action=(
                "Remove unsupported requirements or select a surface that exposes them."
            ),
            missing=missing,
        )
    return descriptor


def capability_contract_manifest() -> dict[str, object]:
    """Return the versioned negotiation and error contract."""
    return {
        "contract_version": CAPABILITY_CONTRACT_VERSION,
        "tool_schema_version": TOOL_SCHEMA_VERSION,
        "compatibility": {
            "major": "must match",
            "minor": "available must be greater than or equal to required",
            "interfaces": "all required values must be present",
            "features": "all required values must be present",
        },
        "error_codes": list(CAPABILITY_ERROR_CODES),
    }


def _normalized(values: Tuple[str, ...]) -> Tuple[str, ...]:
    return tuple(sorted(set(values)))


def _parse_version(value: str) -> Tuple[int, int]:
    parts = value.split(".")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise ValueError(f"Capability versions must use major.minor: {value!r}")
    return int(parts[0]), int(parts[1])


def _compatible_version(available: str, required: str) -> bool:
    available_major, available_minor = _parse_version(available)
    required_major, required_minor = _parse_version(required)
    return available_major == required_major and available_minor >= required_minor


def _integer(metadata: Mapping[str, object], key: str) -> int:
    value = metadata.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Registry capability {key!r} must be an integer.")
    return value


__all__ = [
    "CAPABILITY_CONTRACT_VERSION",
    "CAPABILITY_ERROR_CODES",
    "TOOL_SCHEMA_VERSION",
    "CapabilityDescriptor",
    "CapabilityNegotiationError",
    "CapabilityRequirement",
    "capability_contract_manifest",
    "descriptor_from_registry",
    "installed_sdk_version",
    "negotiate_capabilities",
]
