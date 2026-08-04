"""Typed request contracts for canonical Permutive actions."""

from __future__ import annotations

from typing import List

from typing_extensions import NotRequired, TypedDict

from .sdk import JSONObject


class AliasPayload(TypedDict):
    """External identifier attached to one user."""

    id: str
    tag: str
    priority: NotRequired[int]


class IdentityPayload(TypedDict):
    """Request payload used to identify one user."""

    user_id: str
    aliases: List[AliasPayload]


class EventPayload(TypedDict):
    """Event supplied to user segmentation."""

    name: str
    time: str
    properties: JSONObject
    session_id: NotRequired[str]
    view_id: NotRequired[str]


class SegmentationPayload(TypedDict):
    """Request payload used for user segmentation."""

    events: List[EventPayload]
    user_id: NotRequired[str]
    alias: NotRequired[AliasPayload]


class ContextPayload(TypedDict):
    """Request payload used for context segmentation."""

    url: str
    page_properties: JSONObject
