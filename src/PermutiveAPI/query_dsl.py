"""Composable builders for native Permutive query payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple, Union

Scalar = Union[str, int, float, bool]


@dataclass(frozen=True)
class QueryExpression:
    """Immutable query expression with deterministic serialization."""

    payload: Dict[str, Any]

    def to_json(self) -> Dict[str, Any]:
        """Return a detached JSON-compatible payload."""
        return _copy(self.payload)

    def __and__(self, other: "QueryExpression") -> "QueryExpression":
        return QueryExpression({"and": [self.to_json(), other.to_json()]})

    def __or__(self, other: "QueryExpression") -> "QueryExpression":
        return QueryExpression({"or": [self.to_json(), other.to_json()]})


def _copy(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy(item) for item in value]
    return value


def event(
    name: str,
    *,
    operator: str = "greater_than_or_equal_to",
    times: Union[int, float] = 1,
    where: Optional[QueryExpression] = None,
) -> QueryExpression:
    """Build an event-frequency expression."""
    if not name:
        raise ValueError("event name must not be empty")
    payload: Dict[str, Any] = {
        "event": name,
        "frequency": {operator: times},
    }
    if where is not None:
        payload["where"] = where.to_json()
    return QueryExpression(payload)


def property_condition(
    property_name: str,
    operator: str,
    value: Union[Scalar, Tuple[Scalar, ...]],
) -> QueryExpression:
    """Build a property condition expression."""
    if not property_name:
        raise ValueError("property_name must not be empty")
    serialized: Any = list(value) if isinstance(value, tuple) else value
    return QueryExpression(
        {"property": property_name, "condition": {operator: serialized}}
    )


def in_segment(segment: Union[str, int]) -> QueryExpression:
    """Build a first-party segment membership expression."""
    return QueryExpression({"in_segment": segment})


def all_of(expressions: Iterable[QueryExpression]) -> QueryExpression:
    """Combine two or more expressions with AND."""
    items = tuple(expressions)
    if len(items) < 2:
        raise ValueError("all_of requires at least two expressions")
    return QueryExpression({"and": [item.to_json() for item in items]})


def any_of(expressions: Iterable[QueryExpression]) -> QueryExpression:
    """Combine two or more expressions with OR."""
    items = tuple(expressions)
    if len(items) < 2:
        raise ValueError("any_of requires at least two expressions")
    return QueryExpression({"or": [item.to_json() for item in items]})
