"""Strict downstream type-consumption smoke example."""

from __future__ import annotations

from PermutiveAPI import (
    AsyncPermutiveClient,
    AtomicCredentials,
    EndpointContract,
    PermutiveClient,
    QueryExpression,
    RateLimitCoordinator,
    Recording,
    ResponseKind,
    all_of,
    event,
    property_condition,
)
from PermutiveAPI.plugins.codex import CodexPlugin


def create_sync_client(api_key: str) -> PermutiveClient:
    """Create the canonical synchronous client."""
    return PermutiveClient(api_key)


def create_async_client(api_key: str) -> AsyncPermutiveClient:
    """Create the canonical asynchronous client."""
    return AsyncPermutiveClient(api_key)


def create_query() -> QueryExpression:
    """Create one typed query expression."""
    return all_of(
        [
            event("Pageview"),
            property_condition("client.country", "equals", "FR"),
        ]
    )


def create_runtime_controls(
    api_key: str,
) -> tuple[AtomicCredentials, RateLimitCoordinator]:
    """Create shared credential and rate-limit controls."""
    return AtomicCredentials(api_key), RateLimitCoordinator(10.0)


def define_endpoint_contract() -> EndpointContract:
    """Define one supported response contract downstream."""
    return EndpointContract(
        "cohorts.list",
        "GET",
        "/v1/cohorts",
        ResponseKind.PAGE,
    )


def preserve_recording_type(recording: Recording) -> Recording:
    """Confirm that versioned replay evidence is consumable downstream."""
    return recording


def preserve_plugin_type(plugin: CodexPlugin) -> CodexPlugin:
    """Confirm that the documented plugin type is consumable downstream."""
    return plugin
