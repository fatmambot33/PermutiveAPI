"""Tests for API contracts, drift classification, and replay evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from requests import Response

from PermutiveAPI.contracts import (
    DriftKind,
    SchemaDriftError,
    classify_response_schema,
    contract_manifest,
    endpoint_contracts,
    validate_response_schema,
)
from PermutiveAPI.recording import (
    RecordedInteraction,
    Recording,
    RecordingTransport,
    ReplayMismatchError,
    ReplayTransport,
    sanitize_json,
)
from PermutiveAPI.sdk import PermutiveClient, RetryPolicy


def _manifest() -> dict[str, object]:
    decoded = json.loads(Path("contracts/api-contract-v1.json").read_text())
    assert isinstance(decoded, dict)
    return decoded


def test_generated_contract_matches_samples() -> None:
    """Committed API evidence is exactly reproducible from source samples."""
    decoded = json.loads(Path("contracts/api-samples-v1.json").read_text())
    assert isinstance(decoded, dict)
    samples = decoded["samples"]
    assert isinstance(samples, dict)

    assert contract_manifest(samples) == _manifest()
    assert len(endpoint_contracts()) == 25


def test_additive_and_breaking_schema_drift_are_distinct() -> None:
    """New fields remain compatible while removed fields are breaking."""
    schemas = _manifest()["schemas"]
    assert isinstance(schemas, dict)

    additive = classify_response_schema(
        "cohorts.get",
        {"id": "cohort", "name": "Example", "description": "New"},
        schemas,
    )
    assert additive.kind is DriftKind.ADDITIVE
    assert additive.compatible is True
    assert validate_response_schema(
        "cohorts.get",
        {"id": "cohort", "name": "Example", "description": "New"},
        schemas,
    ) == additive

    additive_page = classify_response_schema(
        "cohorts.list",
        {
            "items": [
                {"id": "cohort", "name": "Example", "description": "New"}
            ],
            "continuation": "next",
        },
        schemas,
    )
    assert additive_page.kind is DriftKind.ADDITIVE

    with pytest.raises(SchemaDriftError) as captured:
        validate_response_schema("cohorts.get", {"id": "cohort"}, schemas)
    assert captured.value.drift.kind is DriftKind.BREAKING
    assert captured.value.to_dict()["code"] == "breaking_response_schema_drift"


def test_recording_sanitizes_nested_secrets_and_query_data() -> None:
    """Recording transport stores no query key or sensitive response value."""

    class Transport:
        def request(self, method: str, url: str, **kwargs: object) -> Response:
            del method, kwargs
            response = Response()
            response.status_code = 200
            response.url = url
            response.headers["Content-Type"] = "application/json"
            response.headers["Authorization"] = "secret"
            response._content = json.dumps(
                {
                    "id": "cohort",
                    "nested": {"access-token": "secret", "safe": "value"},
                }
            ).encode()
            return response

    transport = RecordingTransport(Transport())
    transport.request(
        "GET",
        "https://example.test/cohorts-api/v2/cohorts?k=secret#fragment",
    )
    interaction = transport.recording.interactions[0]

    assert interaction.endpoint == "https://example.test/cohorts-api/v2/cohorts"
    assert "Authorization" not in interaction.headers
    assert interaction.body == {
        "id": "cohort",
        "nested": {"access-token": "[REDACTED]", "safe": "value"},
    }
    assert sanitize_json({"password": "secret"}) == {"password": "[REDACTED]"}


def test_recording_replays_through_the_canonical_client() -> None:
    """Versioned replay fixtures exercise the real synchronous client stack."""
    recording = Recording.read(Path("recordings/core-v1.json"))
    replay = ReplayTransport(recording)
    with PermutiveClient(
        "never-recorded",
        base_url="https://api.permutive.test",
        retry_policy=RetryPolicy(max_attempts=1),
        transport=replay,
    ) as client:
        assert client.request("GET", "cohorts-api/v2/cohorts")[
            "continuation"
        ] == "next"
        assert client.request(
            "POST",
            "cohorts-api/v2/cohorts",
            json={"name": "Reviewed"},
        )["api_key"] == "[REDACTED]"
    assert replay.remaining == 0


def test_replay_rejects_order_or_endpoint_drift() -> None:
    """Replay fails closed when requests no longer match recorded evidence."""
    recording = Recording(
        (
            RecordedInteraction(
                "GET",
                "https://example.test/cohorts-api/v2/cohorts",
                200,
                body={"items": []},
            ),
        )
    )
    replay = ReplayTransport(recording)

    with pytest.raises(ReplayMismatchError):
        replay.request("POST", "https://example.test/cohorts-api/v2/cohorts")
