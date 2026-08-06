"""Validate secret-safe recordings and canonical client replay."""

from __future__ import annotations

from pathlib import Path

from PermutiveAPI.recording import Recording, ReplayTransport
from PermutiveAPI.sdk import PermutiveClient, RetryPolicy

RECORDING_PATH = Path("recordings/core-v1.json")


def main() -> int:
    """Validate fixture safety and replay through the canonical sync client."""
    text = RECORDING_PATH.read_text(encoding="utf-8")
    lowered = text.lower()
    for forbidden in (
        "local-secret-key",
        "authorization",
        "set-cookie",
        "?k=",
        "password",
    ):
        if forbidden in lowered:
            raise SystemExit(f"Recording contains forbidden material: {forbidden}")

    recording = Recording.read(RECORDING_PATH)
    replay = ReplayTransport(recording)
    with PermutiveClient(
        "local-secret-key",
        base_url="https://api.permutive.test",
        retry_policy=RetryPolicy(max_attempts=1),
        transport=replay,
    ) as client:
        listed = client.request("GET", "cohorts-api/v2/cohorts")
        created = client.request(
            "POST",
            "cohorts-api/v2/cohorts",
            json={"name": "Reviewed"},
            idempotent=True,
        )

    if listed.get("continuation") != "next":
        raise SystemExit("Replay list response did not match canonical evidence.")
    if created.get("api_key") != "[REDACTED]":
        raise SystemExit("Replay response was not redacted.")
    if replay.remaining:
        raise SystemExit("Replay did not consume every recorded interaction.")
    print(f"Recording validation passed for {len(recording.interactions)} interactions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
