"""Deterministic tests for query, configuration, and security contracts."""

from __future__ import annotations

import pytest

from PermutiveAPI.config import PermutiveConfig, Secret
from PermutiveAPI.query_dsl import all_of, event, in_segment, property_condition


def test_secret_is_redacted() -> None:
    """Ensure credentials cannot leak through string representations."""
    secret = Secret("top-secret")
    assert "top-secret" not in repr(secret)
    assert "top-secret" not in str(secret)


def test_config_uses_explicit_values_before_environment() -> None:
    """Ensure explicit configuration has deterministic precedence."""
    config = PermutiveConfig.from_env(
        {"PERMUTIVE_API_KEY": "environment-key"},
        api_key="explicit-key",
    )
    assert config.api_key.value == "explicit-key"


def test_config_rejects_insecure_remote_url() -> None:
    """Ensure remote endpoints require TLS."""
    with pytest.raises(ValueError, match="HTTPS"):
        PermutiveConfig(api_key=Secret("key"), base_url="http://example.com")


def test_config_allows_explicit_local_development() -> None:
    """Ensure local HTTP requires explicit opt-in."""
    config = PermutiveConfig(
        api_key=Secret("key"),
        base_url="http://localhost:8000",
        allow_insecure_localhost=True,
    )
    assert config.base_url == "http://localhost:8000"


def test_query_builder_serializes_nested_expressions() -> None:
    """Ensure typed builders produce deterministic native payloads."""
    expression = all_of(
        [
            event("pageview"),
            property_condition("url", "contains", "example.com") | in_segment(42),
        ]
    )
    assert expression.to_json() == {
        "and": [
            {
                "event": "pageview",
                "frequency": {"greater_than_or_equal_to": 1},
            },
            {
                "or": [
                    {"property": "url", "condition": {"contains": "example.com"}},
                    {"in_segment": 42},
                ]
            },
        ]
    }


def test_query_serialization_returns_a_detached_payload() -> None:
    """Ensure callers cannot mutate an expression through serialized output."""
    expression = event("pageview")
    payload = expression.to_json()
    payload["event"] = "changed"
    assert expression.to_json()["event"] == "pageview"
