"""Versioned endpoint coverage and structural response contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence

from .sdk import JSONValue

API_CONTRACT_VERSION = 1


class ResponseKind(str, Enum):
    """Supported high-level response shapes."""

    OBJECT = "object"
    PAGE = "page"
    EMPTY = "empty"


@dataclass(frozen=True)
class EndpointContract:
    """Describe one canonical supported HTTP operation."""

    name: str
    method: str
    path_template: str
    response_kind: ResponseKind
    mutating: bool = False

    def __post_init__(self) -> None:
        """Validate and normalize the endpoint contract."""
        method = self.method.upper()
        if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError(f"Unsupported endpoint method: {method}")
        if not self.path_template.startswith("/v1/"):
            raise ValueError("Endpoint paths must start with /v1/.")
        object.__setattr__(self, "method", method)

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable endpoint metadata."""
        return {
            "name": self.name,
            "method": self.method,
            "path_template": self.path_template,
            "response_kind": self.response_kind.value,
            "mutating": self.mutating,
        }


_ENDPOINTS = (
    EndpointContract("cohorts.list", "GET", "/v1/cohorts", ResponseKind.PAGE),
    EndpointContract("cohorts.get", "GET", "/v1/cohorts/{id}", ResponseKind.OBJECT),
    EndpointContract(
        "cohorts.create",
        "POST",
        "/v1/cohorts",
        ResponseKind.OBJECT,
        mutating=True,
    ),
    EndpointContract(
        "cohorts.update",
        "PUT",
        "/v1/cohorts/{id}",
        ResponseKind.OBJECT,
        mutating=True,
    ),
    EndpointContract(
        "cohorts.delete",
        "DELETE",
        "/v1/cohorts/{id}",
        ResponseKind.EMPTY,
        mutating=True,
    ),
    EndpointContract("segments.list", "GET", "/v1/segments", ResponseKind.PAGE),
    EndpointContract("segments.get", "GET", "/v1/segments/{id}", ResponseKind.OBJECT),
    EndpointContract("workspaces.list", "GET", "/v1/workspaces", ResponseKind.PAGE),
    EndpointContract(
        "workspaces.get",
        "GET",
        "/v1/workspaces/{id}",
        ResponseKind.OBJECT,
    ),
)


class SchemaDriftError(ValueError):
    """Report one deterministic structural response incompatibility."""

    def __init__(
        self,
        endpoint: str,
        *,
        expected: str,
        actual: str,
    ) -> None:
        self.endpoint = endpoint
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Response schema drift for {endpoint}: expected {expected}, got {actual}."
        )

    def to_dict(self) -> dict[str, str]:
        """Return structured drift metadata without payload values."""
        return {
            "code": "response_schema_drift",
            "endpoint": self.endpoint,
            "expected": self.expected,
            "actual": self.actual,
            "recommended_action": (
                "Review the upstream response contract and regenerate schema evidence."
            ),
        }


def endpoint_contracts() -> tuple[EndpointContract, ...]:
    """Return canonical endpoint contracts in deterministic order."""
    return tuple(sorted(_ENDPOINTS, key=lambda item: item.name))


def endpoint_contract(name: str) -> EndpointContract:
    """Return one endpoint contract by stable name."""
    for contract in endpoint_contracts():
        if contract.name == name:
            return contract
    raise KeyError(f"Unknown endpoint contract: {name}")


def structural_schema(value: JSONValue) -> str:
    """Return a value-free deterministic structural schema string."""
    return json.dumps(_shape(value), sort_keys=True, separators=(",", ":"))


def schema_fingerprint(value: JSONValue) -> str:
    """Return the SHA-256 fingerprint of a structural response schema."""
    return hashlib.sha256(structural_schema(value).encode("utf-8")).hexdigest()


def contract_manifest(samples: Mapping[str, JSONValue]) -> dict[str, object]:
    """Build versioned endpoint and schema evidence from representative samples."""
    missing = {contract.name for contract in endpoint_contracts()} - set(samples)
    extra = set(samples) - {contract.name for contract in endpoint_contracts()}
    if missing:
        raise ValueError("Missing schema samples: " + ", ".join(sorted(missing)))
    if extra:
        raise ValueError("Unknown schema samples: " + ", ".join(sorted(extra)))
    return {
        "version": API_CONTRACT_VERSION,
        "endpoints": [contract.to_dict() for contract in endpoint_contracts()],
        "schemas": {
            name: {
                "schema": structural_schema(sample),
                "fingerprint": schema_fingerprint(sample),
            }
            for name, sample in sorted(samples.items())
        },
    }


def validate_response_schema(
    endpoint: str,
    payload: JSONValue,
    expected_schemas: Mapping[str, Mapping[str, str]],
) -> None:
    """Raise when one response shape differs from committed evidence."""
    expected = expected_schemas.get(endpoint)
    if expected is None:
        raise KeyError(f"No committed schema for endpoint: {endpoint}")
    expected_fingerprint = expected.get("fingerprint")
    if expected_fingerprint is None:
        raise ValueError(f"Schema evidence has no fingerprint: {endpoint}")
    actual_fingerprint = schema_fingerprint(payload)
    if actual_fingerprint != expected_fingerprint:
        raise SchemaDriftError(
            endpoint,
            expected=expected_fingerprint,
            actual=actual_fingerprint,
        )


def _shape(value: JSONValue) -> object:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        unique = {_schema_key(_shape(item)): _shape(item) for item in value}
        return {"array": [unique[key] for key in sorted(unique)]}
    if isinstance(value, dict):
        return {
            "object": {
                key: _shape(item)
                for key, item in sorted(value.items(), key=lambda pair: pair[0])
            }
        }
    raise TypeError(f"Unsupported JSON value type: {type(value).__name__}")


def _schema_key(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


__all__ = [
    "API_CONTRACT_VERSION",
    "EndpointContract",
    "ResponseKind",
    "SchemaDriftError",
    "contract_manifest",
    "endpoint_contract",
    "endpoint_contracts",
    "schema_fingerprint",
    "structural_schema",
    "validate_response_schema",
]
