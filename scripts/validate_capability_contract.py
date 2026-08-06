"""Validate committed capability negotiation evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from PermutiveAPI.capabilities import capability_contract_manifest

CONTRACT_PATH = Path("capabilities/contract-v1.json")


def validate_capability_contract(path: Path = CONTRACT_PATH) -> None:
    """Raise when committed capability evidence differs from runtime behavior."""
    committed: Any = json.loads(path.read_text(encoding="utf-8"))
    expected = capability_contract_manifest()
    if committed != expected:
        raise SystemExit(
            "Capability contract evidence is stale. Regenerate "
            f"{path.as_posix()} from capability_contract_manifest()."
        )


def main() -> int:
    """Validate the canonical capability contract."""
    validate_capability_contract()
    print("Capability contract validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
