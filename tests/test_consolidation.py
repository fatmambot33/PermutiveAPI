"""Regression tests for the 6.8 consolidation contracts."""

from __future__ import annotations

import json

import PermutiveAPI
from PermutiveAPI.client import PermutiveClient
from PermutiveAPI.consolidation import consolidation_fingerprint, consolidation_json
from PermutiveAPI.contracts import endpoint_contracts
from PermutiveAPI.integration_registry import integration_registry_manifest
from PermutiveAPI.public_surface import classify_public_export, public_surface_manifest
from PermutiveAPI.resource_registry import (
    ResourceDefinition,
    ResourceOperation,
    resource_definitions,
    resource_registry_manifest,
)


def test_resource_registry_matches_endpoint_contracts() -> None:
    """Ensure canonical endpoint evidence cannot drift from the registry."""
    manifest = resource_registry_manifest()
    resources = manifest["resources"]
    assert isinstance(resources, list)
    registry_endpoints = {
        f"{resource['name']}.{operation['name']}": (
            operation["method"],
            f"{resource['path']}{operation['suffix']}",
            operation["response_kind"],
            operation["mutating"],
        )
        for resource in resources
        for operation in resource["operations"]
    }
    contract_endpoints = {
        contract.name: (
            contract.method,
            contract.path_template,
            contract.response_kind.value,
            contract.mutating,
        )
        for contract in endpoint_contracts()
    }
    assert manifest["operation_count"] == 25
    assert registry_endpoints == contract_endpoints


def test_runtime_client_paths_are_driven_by_resource_registry() -> None:
    """Ensure runtime resource paths use the canonical registry definitions."""
    client = PermutiveClient("api-key")
    runtime_paths = {
        resource.name: getattr(client, resource.name).path
        for resource in resource_definitions()
    }
    registry_paths = {
        resource.name: resource.path.lstrip("/") for resource in resource_definitions()
    }
    assert runtime_paths == registry_paths


def test_resource_registry_rejects_duplicate_operations() -> None:
    """Ensure duplicate canonical operations fail before generation."""
    operation = ResourceOperation("get", "GET", "/{id}", "object")
    try:
        ResourceDefinition("items", "/items", (operation, operation))
    except ValueError as error:
        assert "Duplicate operations" in str(error)
    else:
        raise AssertionError("duplicate operations must be rejected")


def test_public_surface_manifest_classifies_every_export_once() -> None:
    """Ensure the package-root surface has complete deterministic evidence."""
    manifest = public_surface_manifest(PermutiveAPI.__all__)
    classifications = manifest["classifications"]
    assert isinstance(classifications, dict)
    flattened = [
        name
        for category in ("canonical", "integration", "compatibility")
        for name in classifications[category]
    ]
    assert manifest["export_count"] == len(PermutiveAPI.__all__)
    assert sorted(flattened) == sorted(PermutiveAPI.__all__)
    assert len(flattened) == len(set(flattened))
    assert classify_public_export("PermutiveAPIError") == "compatibility"
    assert classify_public_export("CAPABILITY_CONTRACT_VERSION") == "integration"


def test_public_surface_rejects_unclassified_exports() -> None:
    """Ensure new package-root exports require an explicit classification."""
    try:
        classify_public_export("FutureUnclassifiedExport")
    except ValueError as error:
        assert "Unclassified" in str(error)
    else:
        raise AssertionError("unclassified exports must be rejected")


def test_integration_registry_reports_governance_per_surface() -> None:
    """Ensure integration metadata reflects the actual invocation contract."""
    manifest = integration_registry_manifest()
    surfaces = manifest["surfaces"]
    assert isinstance(surfaces, list)
    by_name = {surface["name"]: surface for surface in surfaces}
    assert sorted(by_name) == ["agent", "mcp", "plugin", "tools"]
    assert by_name["tools"]["governed"] is False
    assert by_name["agent"]["governed"] is True
    assert by_name["plugin"]["governed"] is True


def test_consolidation_evidence_is_reproducible() -> None:
    """Ensure first-class SDK evidence is byte-for-byte deterministic."""
    first = consolidation_json(PermutiveAPI.__all__)
    second = consolidation_json(tuple(reversed(PermutiveAPI.__all__)))
    assert json.loads(first) == json.loads(second)
    assert first == second
    assert consolidation_fingerprint(PermutiveAPI.__all__) == consolidation_fingerprint(
        tuple(reversed(PermutiveAPI.__all__))
    )
