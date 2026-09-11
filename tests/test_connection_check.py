"""Contract tests for credential and live connection readiness checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from PermutiveAPI import cli
from PermutiveAPI.credentials import Credentials, CredentialsError
from PermutiveAPI.sdk import AuthenticationError


class StaticProvider:
    """Return one deterministic credential without reading local state."""

    def __init__(self, *_: Any, **__: Any) -> None:
        self.credentials = Credentials("secret-key", "environment:PERMUTIVE_API_KEY")

    def load(self) -> Credentials:
        """Return deterministic credentials."""
        return self.credentials


class MissingProvider:
    """Raise the normal local credential resolution error."""

    def __init__(self, *_: Any, **__: Any) -> None:
        pass

    def load(self) -> Credentials:
        """Raise a credential error without a secret value."""
        raise CredentialsError("missing")


class Cohorts:
    """Small cohort resource double used by the connection probe."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.page_sizes: list[int] = []

    def list(self, *, page_size: int = 100) -> object:
        """Record the bounded read or raise the configured error."""
        self.page_sizes.append(page_size)
        if self.error is not None:
            raise self.error
        return object()


class FakeClient:
    """Context-managed client double for the read-only connection probe."""

    error: Exception | None = None
    last_api_key: str | None = None
    cohorts_instance: Cohorts | None = None

    def __init__(self, api_key: str) -> None:
        type(self).last_api_key = api_key
        type(self).cohorts_instance = Cohorts(error=type(self).error)
        self.cohorts = type(self).cohorts_instance

    def __enter__(self) -> "FakeClient":
        """Return the active fake client."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the fake context without suppressing errors."""
        return None


def test_connection_report_checks_credentials_and_safe_read(monkeypatch) -> None:
    """Resolve credentials and prove connectivity with one bounded read."""
    monkeypatch.setattr(cli, "LocalCredentialsProvider", StaticProvider)
    monkeypatch.setattr(cli, "PermutiveClient", FakeClient)
    FakeClient.error = None

    report = cli.connection_report(Path(".env"))

    assert report["ok"] is True
    assert report["credentials"] == {"status": "ok", "source": "environment"}
    assert report["connection"] == {"status": "ok"}
    assert FakeClient.last_api_key == "secret-key"
    assert FakeClient.cohorts_instance is not None
    assert FakeClient.cohorts_instance.page_sizes == [1]
    assert "secret-key" not in repr(report)


def test_connection_report_guides_missing_credentials(monkeypatch) -> None:
    """Return one stable repair path when credentials cannot be resolved."""
    monkeypatch.setattr(cli, "LocalCredentialsProvider", MissingProvider)

    report = cli.connection_report(Path(".env"))

    assert report["ok"] is False
    assert report["error_code"] == "credentials_missing"
    assert report["connection"] == {"status": "not_checked"}
    assert "permutiveapi configure" in str(report["recommended_action"])


def test_connection_report_classifies_authentication_failure(monkeypatch) -> None:
    """Keep authentication failures actionable without exposing the API key."""
    monkeypatch.setattr(cli, "LocalCredentialsProvider", StaticProvider)
    monkeypatch.setattr(cli, "PermutiveClient", FakeClient)
    FakeClient.error = AuthenticationError("do-not-expose", status_code=401)

    report = cli.connection_report(Path(".env"))

    assert report["ok"] is False
    assert report["credentials"] == {"status": "ok", "source": "environment"}
    assert report["connection"] == {"status": "failed"}
    assert report["error_code"] == "authentication_failed"
    assert "secret-key" not in repr(report)
    assert "do-not-expose" not in repr(report)
