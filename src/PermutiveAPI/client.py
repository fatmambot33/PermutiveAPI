"""Simple resource-oriented public client."""

from __future__ import annotations

from functools import cached_property

from .resources import Resource
from .sdk import JSONObject, PermutiveClient as TransportClient


def _decode_object(payload: JSONObject) -> JSONObject:
    """Return one decoded JSON object unchanged."""
    return payload


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
        return Resource(self, "cohorts-api/v2/cohorts", _decode_object)

    @cached_property
    def imports(self) -> Resource[JSONObject]:
        """Return audience import operations."""
        return Resource(self, "audiences-api/v1/imports", _decode_object)

    @cached_property
    def segments(self) -> Resource[JSONObject]:
        """Return audience segment operations."""
        return Resource(self, "audiences-api/v1/segments", _decode_object)

    @cached_property
    def sources(self) -> Resource[JSONObject]:
        """Return audience source operations."""
        return Resource(self, "audiences-api/v1/sources", _decode_object)

    @cached_property
    def workspaces(self) -> Resource[JSONObject]:
        """Return workspace operations."""
        return Resource(self, "workspaces-api/v1/workspaces", _decode_object)
