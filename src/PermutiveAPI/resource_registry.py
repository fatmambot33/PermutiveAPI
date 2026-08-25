"""Single typed source of truth for canonical resource operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Tuple


@dataclass(frozen=True)
class ResourceOperation:
    """Describe one operation supported by a canonical resource."""

    name: str
    method: str
    suffix: str
    response_kind: str
    mutating: bool = False

    def __post_init__(self) -> None:
        """Validate stable operation metadata."""
        method = self.method.upper()
        if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError(f"Unsupported resource method: {method}")
        if self.suffix and not self.suffix.startswith("/"):
            raise ValueError("Resource operation suffixes must be empty or absolute.")
        if self.response_kind not in {"object", "page", "empty"}:
            raise ValueError(f"Unsupported response kind: {self.response_kind}")
        object.__setattr__(self, "method", method)

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable metadata."""
        return {
            "name": self.name,
            "method": self.method,
            "suffix": self.suffix,
            "response_kind": self.response_kind,
            "mutating": self.mutating,
        }


@dataclass(frozen=True)
class ResourceDefinition:
    """Describe one canonical Permutive resource surface."""

    name: str
    path: str
    operations: Tuple[ResourceOperation, ...]

    def __post_init__(self) -> None:
        """Validate deterministic resource metadata."""
        if not self.name or not self.name.isidentifier():
            raise ValueError(f"Invalid resource name: {self.name!r}")
        if not self.path.startswith("/"):
            raise ValueError("Resource paths must be absolute API paths.")
        names = tuple(operation.name for operation in self.operations)
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate operations for resource {self.name}.")

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable metadata."""
        return {
            "name": self.name,
            "path": self.path,
            "operations": [operation.to_dict() for operation in self.operations],
        }


_CANONICAL_OPERATIONS: Final[Tuple[ResourceOperation, ...]] = (
    ResourceOperation("list", "GET", "", "page"),
    ResourceOperation("get", "GET", "/{id}", "object"),
    ResourceOperation("create", "POST", "", "object", True),
    ResourceOperation("update", "PATCH", "/{id}", "object", True),
    ResourceOperation("delete", "DELETE", "/{id}", "empty", True),
)

_RESOURCES: Final[Tuple[ResourceDefinition, ...]] = (
    ResourceDefinition("cohorts", "/cohorts-api/v2/cohorts", _CANONICAL_OPERATIONS),
    ResourceDefinition("imports", "/audiences-api/v1/imports", _CANONICAL_OPERATIONS),
    ResourceDefinition("segments", "/audiences-api/v1/segments", _CANONICAL_OPERATIONS),
    ResourceDefinition("sources", "/audiences-api/v1/sources", _CANONICAL_OPERATIONS),
    ResourceDefinition(
        "workspaces", "/workspaces-api/v1/workspaces", _CANONICAL_OPERATIONS
    ),
)


def resource_definitions() -> Tuple[ResourceDefinition, ...]:
    """Return canonical resources in deterministic order."""
    return tuple(sorted(_RESOURCES, key=lambda resource: resource.name))


def resource_definition(name: str) -> ResourceDefinition:
    """Return one canonical resource by stable name."""
    for resource in resource_definitions():
        if resource.name == name:
            return resource
    raise KeyError(f"Unknown canonical resource: {name}")


def resource_registry_manifest() -> dict[str, object]:
    """Return the versioned canonical resource registry."""
    return {
        "version": 1,
        "resources": [resource.to_dict() for resource in resource_definitions()],
        "operation_count": sum(
            len(resource.operations) for resource in resource_definitions()
        ),
    }


__all__ = [
    "ResourceDefinition",
    "ResourceOperation",
    "resource_definition",
    "resource_definitions",
    "resource_registry_manifest",
]
