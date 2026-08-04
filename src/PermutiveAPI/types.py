"""Shared static types for the public SDK."""

from __future__ import annotations

from typing import Dict, List, TypeAlias, Union

JSONScalar: TypeAlias = Union[str, int, float, bool, None]
JSONValue: TypeAlias = Union[JSONScalar, "JSONObject", "JSONArray"]
JSONObject: TypeAlias = Dict[str, JSONValue]
JSONArray: TypeAlias = List[JSONValue]

__all__ = ["JSONArray", "JSONObject", "JSONScalar", "JSONValue"]
