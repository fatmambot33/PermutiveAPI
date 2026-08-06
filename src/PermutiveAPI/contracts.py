"""Versioned endpoint coverage and structural response contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from .sdk import JSONValue

API_CONTRACT_VERSION = 1


class ResponseKind(str, Enum):
    """Supported high-level response shapes."""

    OBJECT = "object"
    PAGE = "page"
    EMPTY = "empty"


class DriftKind(str, Enum):
    """Compatibility classifications for structural response drift."""

    NONE = "none"
    ADDITIVE = "additive"
    BREAKING = "breaking"


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
        if not self.path_template.startswith("/"):
            raise ValueError("Endpoint paths must be absolute API paths.")
        if "?" in self.path_template or "#" in self.path_template:
            raise ValueError("Endpoint paths must not contain query or fragment data.")
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


@dataclass(frozen=True)
class SchemaDrift:
    """Describe value-free compatibility evidence for one response shape."""

    endpoint: str
    kind: DriftKind
    expected_fingerprint: str
    actual_fingerprint: str

    @property
    def compatible(self) -> bool:
        """Return whether consumers remain structurally compatible."""
        return self.kind is not DriftKind.BREAKING

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable drift evidence."""
        return {
            "endpoint": self.endpoint,
            "kind": self.kind.value,
            "compatible": self.compatible,
            "expected_fingerprint": self.expected_fingerprint,
            "actual_fingerprint": self.actual_fingerprint,
        }


_RESOURCE_PATHS: Mapping[str, str] = {
    "cohorts": "/cohorts-api/v2/cohorts",
    "imports": "/audiences-api/v1/imports",
    "segments": "/audiences-api/v1/segments",
    "sources": "/audiences-api/v1/sources",
    "workspaces": "/workspaces-api/v1/workspaces",
}
_RESOURCE_OPERATIONS: tuple[
    tuple[str, str, str, ResponseKind, bool],
    ...,
] = (
    ("list", "GET", "", ResponseKind.PAGE, False),
    ("get", "GET", "/{id}", ResponseKind.OBJECT, False),
    ("create", "POST", "", ResponseKind.OBJECT, True),
    ("update", "PATCH", "/{id}", ResponseKind.OBJECT, True),
    ("delete", "DELETE", "/{id}", ResponseKind.EMPTY, True),
)
_ENDPOINTS = tuple(
    EndpointContract(
        name=f"{resource}.{operation}",
        method=method,
        path_template=f"{path}{suffix}",
        response_kind=response_kind,
        mutating=mutating,
    )
    for resource, path in _RESOURCE_PATHS.items()
    for operation, method, suffix, response_kind, mutating in _RESOURCE_OPERATIONS
)


class SchemaDriftError(ValueError):
    """Report one deterministic breaking response incompatibility."""

    def __init__(self, drift: SchemaDrift) -> None:
        self.drift = drift
        super().__init__(
            f"Breaking response schema drift for {drift.endpoint}: "
            f"expected {drift.expected_fingerprint}, "
            f"got {drift.actual_fingerprint}."
        )

    def to_dict(self) -> dict[str, object]:
        """Return structured drift metadata without payload values."""
        result = self.drift.to_dict()
        result.update(
            {
                "code": "breaking_response_schema_drift",
                "recommended_action": (
                    "Review the upstream response contract and regenerate schema evidence."
                ),
            }
        )
        return result


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
    expected_names = {contract.name for contract in endpoint_contracts()}
    missing = expected_names - set(samples)
    extra = set(samples) - expected_names
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


def classify_response_schema(
    endpoint: str,
    payload: JSONValue,
    expected_schemas: Mapping[str, Mapping[str, str]],
) -> SchemaDrift:
    """Classify one actual response as unchanged, additive, or breaking."""
    expected = expected_schemas.get(endpoint)
    if expected is None:
        raise KeyError(f"No committed schema for endpoint: {endpoint}")
    expected_schema = expected.get("schema")
    expected_fingerprint = expected.get("fingerprint")
    if expected_schema is None or expected_fingerprint is None:
        raise ValueError(f"Schema evidence is incomplete: {endpoint}")
    expected_shape: object = json.loads(expected_schema)
    actual_shape = _shape(payload)
    return SchemaDrift(
        endpoint=endpoint,
        kind=_classify_shape(expected_shape, actual_shape),
        expected_fingerprint=expected_fingerprint,
        actual_fingerprint=schema_fingerprint(payload),
    )


def validate_response_schema(
    endpoint: str,
    payload: JSONValue,
    expected_schemas: Mapping[str, Mapping[str, str]],
) -> SchemaDrift:
    """Return drift evidence and raise only for breaking response changes."""
    drift = classify_response_schema(endpoint, payload, expected_schemas)
    if drift.kind is DriftKind.BREAKING:
        raise SchemaDriftError(drift)
    return drift


def _classify_shape(expected: object, actual: object) -> DriftKind:
    if expected == actual:
        return DriftKind.NONE
    if isinstance(expected, dict) and isinstance(actual, dict):
        if set(expected) != set(actual):
            return DriftKind.BREAKING
        if "object" in expected and "object" in actual:
            expected_fields = expected["object"]
            actual_fields = actual["object"]
            if not isinstance(expected_fields, dict) or not isinstance(actual_fields, dict):
                return DriftKind.BREAKING
            if not set(expected_fields).issubset(actual_fields):
                return DriftKind.BREAKING
            kinds = [
                _classify_shape(expected_fields[key], actual_fields[key])
                for key in expected_fields
            ]
            if DriftKind.BREAKING in kinds:
                return DriftKind.BREAKING
            if set(actual_fields) - set(expected_fields) or DriftKind.ADDITIVE in kinds:
                return DriftKind.ADDITIVE
            return DriftKind.NONE
        if "array" in expected and "array" in actual:
            expected_items = expected["array"]
            actual_items = actual["array"]
            if not isinstance(expected_items, list) or not isinstance(actual_items, list):
                return DriftKind.BREAKING
            return _classify_array(expected_items, actual_items)
    return DriftKind.BREAKING


def _classify_array(expected_items: list[object], actual_items: list[object]) -> DriftKind:
    unmatched = list(actual_items)
    additive = False
    for expected_item in expected_items:
        match_index: int | None = None
        match_kind = DriftKind.BREAKING
        for index, actual_item in enumerate(unmatched):
            kind = _classify_shape(expected_item, actual_item)
            if kind is DriftKind.NONE:
                match_index = index
                match_kind = kind
                break
            if kind is DriftKind.ADDITIVE and match_index is None:
                match_index = index
                match_kind = kind
        if match_index is None:
            return DriftKind.BREAKING
        unmatched.pop(match_index)
        additive = additive or match_kind is DriftKind.ADDITIVE
    if unmatched:
        additive = True
    return DriftKind.ADDITIVE if additive else DriftKind.NONE


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
    "DriftKind",
    "EndpointContract",
    "ResponseKind",
    "SchemaDrift",
    "SchemaDriftError",
    "classify_response_schema",
    "contract_manifest",
    "endpoint_contract",
    "endpoint_contracts",
    "schema_fingerprint",
    "structural_schema",
    "validate_response_schema",
]
