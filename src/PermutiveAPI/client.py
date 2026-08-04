"""Typed, dependency-injectable SDK transport client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Protocol, Tuple

import requests
from requests import Response, Session

from .types import JSONObject
from .utils.http import raise_for_status


class Transport(Protocol):
    """Minimal injectable HTTP transport contract."""

    def request(self, method: str, url: str, **kwargs: object) -> Response:
        """Send an HTTP request."""
        ...


@dataclass(frozen=True)
class ClientConfig:
    """Immutable client configuration."""

    api_key: str
    base_url: str = "https://api.permutive.com"
    timeout: Tuple[float, float] = (3.05, 30.0)


class PermutiveClient:
    """Small typed client used as the canonical transport boundary."""

    def __init__(
        self,
        config: ClientConfig,
        *,
        transport: Optional[Transport] = None,
    ) -> None:
        self.config = config
        self._transport = transport or Session()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, object]] = None,
        json: Optional[JSONObject] = None,
        idempotent: Optional[bool] = None,
    ) -> Response:
        """Send one request with explicit timeout and idempotency semantics."""
        normalized = method.upper()
        if normalized not in {"GET", "POST", "PATCH", "DELETE"}:
            raise ValueError(f"Unsupported HTTP method: {method}")

        safe = normalized in {"GET", "DELETE"}
        if idempotent is None:
            idempotent = safe
        if normalized in {"POST", "PATCH"} and idempotent:
            raise ValueError(
                "Unsafe writes require an endpoint-specific idempotency mechanism."
            )

        request_params = dict(params or {})
        request_params["k"] = self.config.api_key
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"

        try:
            response = self._transport.request(
                normalized,
                url,
                params=request_params,
                json=json,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=self.config.timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeError("Permutive transport request failed") from exc

        if not 200 <= response.status_code < 300:
            raise_for_status(requests.RequestException("HTTP request failed"), response)
        return response

    def close(self) -> None:
        """Close the underlying session when supported."""
        close = getattr(self._transport, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "PermutiveClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


__all__ = ["ClientConfig", "PermutiveClient", "Transport"]
