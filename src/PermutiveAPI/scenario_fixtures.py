"""Versioned local HTTP fixtures for governed scenario recipes."""

from __future__ import annotations

from typing import Tuple

from .testing import MockResponse, MockRoute

SCENARIO_FIXTURE_VERSION = 1


def scenario_mock_routes() -> Tuple[MockRoute, ...]:
    """Return deterministic HTTP routes used by governed scenarios."""
    return (
        MockRoute(
            "GET",
            "/v1/workspaces/current",
            (
                MockResponse(
                    body={
                        "id": "workspace-fixture",
                        "name": "Fixture Workspace",
                    }
                ),
            ),
        ),
        MockRoute(
            "GET",
            "/v1/cohorts",
            (
                MockResponse(
                    body={
                        "items": [
                            {"id": "cohort-alpha", "name": "Alpha"},
                            {"id": "cohort-beta", "name": "Beta"},
                        ]
                    }
                ),
            ),
        ),
        MockRoute(
            "GET",
            "/v1/segments/left",
            (MockResponse(body={"id": "segment-left", "size": 120}),),
        ),
        MockRoute(
            "GET",
            "/v1/segments/right",
            (MockResponse(body={"id": "segment-right", "size": 80}),),
        ),
        MockRoute(
            "POST",
            "/v1/cohorts/reviewed",
            (
                MockResponse(
                    status_code=201,
                    body={
                        "id": "cohort-reviewed",
                        "state": "reviewed",
                    },
                ),
            ),
        ),
        MockRoute(
            "GET",
            "/v1/server-failure",
            (MockResponse(status_code=500, body={"error": "server_failure"}),),
        ),
    )


def scenario_fixture_catalog() -> dict[str, object]:
    """Return the machine-readable governed-scenario fixture catalog."""
    return {
        "version": SCENARIO_FIXTURE_VERSION,
        "routes": [route.to_dict() for route in scenario_mock_routes()],
    }


__all__ = [
    "SCENARIO_FIXTURE_VERSION",
    "scenario_fixture_catalog",
    "scenario_mock_routes",
]
