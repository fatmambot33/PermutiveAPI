"""Tests for versioned capability discovery and negotiation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import pytest

from PermutiveAPI.capabilities import (
    CapabilityDescriptor,
    CapabilityNegotiationError,
    CapabilityRequirement,
    capability_contract_manifest,
    descriptor_from_registry,
)


class _Registry:
    def capabilities(self) -> Mapping[str, object]:
        return {
            "tool_count": 3,
            "read_only_tools": 2,
            "writable_tools": 1,
        }


def test_contract_manifest_matches_committed_evidence() -> None:
    """The negotiation contract cannot drift from committed JSON evidence."""
    committed = json.loads(
        Path("capabilities/contract-v1.json").read_text(encoding="utf-8")
    )

    assert committed == capability_contract_manifest()


def test_registry_descriptor_is_deterministic() -> None:
    """Registry counts derive stable interfaces and features."""
    descriptor = descriptor_from_registry(
        _Registry(),
        surface="registry",
        interfaces=("json_schema", "openai_tools"),
        features=("custom",),
    )

    assert descriptor.surface == "registry"
    assert descriptor.interfaces == ("json_schema", "openai_tools")
    assert descriptor.features == (
        "custom",
        "read_tools",
        "tool_discovery",
        "tool_invocation",
        "write_tools",
    )
    assert descriptor.tool_count == 3
    assert descriptor.read_only_tools == 2
    assert descriptor.writable_tools == 1


def test_compatible_requirement_returns_descriptor() -> None:
    """Compatible requirements negotiate before execution."""
    descriptor = CapabilityDescriptor(
        surface="plugin",
        sdk_version="6.6.0",
        interfaces=("plugin", "openai_tools"),
        features=("read_tools", "write_tools"),
        plugin_api_version="1.1",
        tool_count=2,
        read_only_tools=1,
        writable_tools=1,
    )
    requirement = CapabilityRequirement(
        contract_version="1.0",
        tool_schema_version="1.0",
        plugin_api_version="1.0",
        interfaces=("plugin",),
        features=("read_tools",),
    )

    assert descriptor.negotiate(requirement) is descriptor


@pytest.mark.parametrize(
    ("descriptor", "requirement", "code"),
    (
        (
            CapabilityDescriptor(
                surface="registry",
                sdk_version="6.6.0",
                interfaces=(),
                features=(),
                contract_version="2.0",
            ),
            CapabilityRequirement(contract_version="1.0"),
            "capability_contract_incompatible",
        ),
        (
            CapabilityDescriptor(
                surface="registry",
                sdk_version="6.6.0",
                interfaces=(),
                features=(),
                tool_schema_version="1.0",
            ),
            CapabilityRequirement(tool_schema_version="1.1"),
            "tool_schema_incompatible",
        ),
        (
            CapabilityDescriptor(
                surface="plugin",
                sdk_version="6.6.0",
                interfaces=("plugin",),
                features=(),
                plugin_api_version="1.0",
            ),
            CapabilityRequirement(plugin_api_version="2.0"),
            "plugin_api_incompatible",
        ),
        (
            CapabilityDescriptor(
                surface="registry",
                sdk_version="6.6.0",
                interfaces=("json_schema",),
                features=("read_tools",),
            ),
            CapabilityRequirement(
                interfaces=("mcp",),
                features=("write_tools",),
            ),
            "capability_missing",
        ),
    ),
)
def test_incompatible_requirements_use_stable_error_codes(
    descriptor: CapabilityDescriptor,
    requirement: CapabilityRequirement,
    code: str,
) -> None:
    """Negotiation failures are structured and actionable."""
    with pytest.raises(CapabilityNegotiationError) as captured:
        descriptor.negotiate(requirement)

    error = captured.value
    assert error.code == code
    assert error.recommended_action
    assert error.to_dict()["code"] == code


def test_versions_require_major_minor_format() -> None:
    """Ambiguous capability versions fail at construction time."""
    with pytest.raises(ValueError, match="major.minor"):
        CapabilityRequirement(contract_version="1")
