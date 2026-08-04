"""Regression tests for the supported SDK behavior contract."""

from __future__ import annotations

from inspect import signature

from PermutiveAPI import PermutiveClient, Resource


def test_client_keeps_supported_resource_namespaces() -> None:
    """Protect the canonical resource-oriented client surface."""
    expected = {"cohorts", "imports", "segments", "sources", "workspaces"}
    assert expected <= set(vars(PermutiveClient))


def test_resource_keeps_canonical_operations() -> None:
    """Protect the common CRUD, pagination, and iteration vocabulary."""
    expected = {
        "get",
        "list",
        "list_page",
        "iter_all",
        "create",
        "update",
        "delete",
    }
    assert expected <= set(vars(Resource))


def test_resource_required_parameter_shapes_are_stable() -> None:
    """Catch incompatible changes to required public parameters."""
    assert tuple(signature(Resource.get).parameters) == ("self", "resource_id")
    assert tuple(signature(Resource.create).parameters) == ("self", "payload")
    assert tuple(signature(Resource.update).parameters) == (
        "self",
        "resource_id",
        "payload",
    )
    assert tuple(signature(Resource.delete).parameters) == ("self", "resource_id")


def test_list_alias_matches_canonical_list_signature() -> None:
    """Keep the compatibility alias behaviorally aligned with list."""
    assert signature(Resource.list_page) == signature(Resource.list)
