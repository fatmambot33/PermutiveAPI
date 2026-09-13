"""Regression tests for the trusted Codex review-gate workflow."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "codex-review-gate.yml"
FRAMEWORK_REVISION = "51f82dfc0ae99ad1030ded032df3b724c8b7d0df"


def test_codex_gate_uses_trusted_status_based_orchestration() -> None:
    """Keep the merge gate bound to trusted PR-head and live-base evidence."""
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "pull_request_target:" in workflow
    assert "pull-requests: read" in workflow
    assert "statuses: write" in workflow
    assert "checks: write" not in workflow
    assert "cancel-in-progress: true" in workflow
    assert 'commits/${BASE_REF}' in workflow
    assert "head-sha: ${{ github.event.pull_request.head.sha }}" in workflow
    assert "review-context: ${{ steps.base.outputs.sha }}" in workflow
    assert "mode: request-and-wait" in workflow
    assert "check-name: codex-review" in workflow
    assert (
        "fatmambot33/ai-native-platform/actions/codex-review-gate@"
        f"{FRAMEWORK_REVISION}" in workflow
    )
