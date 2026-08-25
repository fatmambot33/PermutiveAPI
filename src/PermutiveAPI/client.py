"""Simple resource-oriented public client."""

from __future__ import annotations

from functools import cached_property

from .resource_registry import resource_definition
from .resources import Resource
from .sdk import JSONObject, PermutiveClient as TransportClient


def _decode_object(payload: JSONObject) -> JSONObject:
    """Return one decoded JSON object unchanged."""
    return payload


def _resource_path(name: str) -> str:
    """Return the transport-relative path for one canonical resource."""
    return resource_definition(name).path.lstrip("/")


class PermutiveClient(TransportClient):
    """Permutive client with one predictable entry point per resource.

    Examples
    --------
    >>> client = PermutiveClient("api-key")
    >>> cohort = client.cohorts.get("cohort-id")
    >>> page = client.segments.list()
    """

    @cached_property
    def cohorts(self) -> Resource[JSONObject]:
        """Return cohort operations."""
        return Resource(self, _resource_path("cohorts"), _decode_object)

    @cached_property
    def imports(self) -> Resource[JSONObject]:
        """Return audience import operations."""
        return Resource(self, _resource_path("imports"), _decode_object)

    @cached_property
    def segments(self) -> Resource[JSONObject]:
        """Return audience segment operations."""
        return Resource(self, _resource_path("segments"), _decode_object)

    @cached_property
    def sources(self) -> Resource[JSONObject]:
        """Return audience source operations."""
        return Resource(self, _resource_path("sources"), _decode_object)

    @cached_property
    def workspaces(self) -> Resource[JSONObject]:
        """Return workspace operations."""
        return Resource(self, _resource_path("workspaces"), _decode_object)
