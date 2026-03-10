"""Typed Pydantic models for native Permutive query payloads."""

from __future__ import annotations

import json
import sys
from typing import Any, Literal, Union

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from typing_extensions import StrEnum  # type: ignore[attr-defined]

from typing_extensions import TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationError,
    field_validator,
    model_validator,
)

PermutiveScalar: TypeAlias = Union[str, int, float, bool]
ConditionValue: TypeAlias = Union[
    PermutiveScalar, "list[PermutiveScalar]", "dict[str, PermutiveScalar]"
]


class FrequencyOperator(StrEnum):
    """Operators allowed for event and engagement frequency checks."""

    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL_TO = "greater_than_or_equal_to"
    EQUAL_TO = "equal_to"
    LESS_THAN_OR_EQUAL_TO = "less_than_or_equal_to"
    LESS_THAN = "less_than"


class TimeUnit(StrEnum):
    """Units allowed in ``during.the_last`` windows."""

    DAY = "day"
    DAYS = "days"
    WEEK = "week"
    WEEKS = "weeks"
    MONTH = "month"
    MONTHS = "months"
    HOUR = "hour"
    HOURS = "hours"
    MINUTE = "minute"
    MINUTES = "minutes"


class WhereFunction(StrEnum):
    """Aggregate functions allowed in functional where conditions."""

    ANY = "any"
    ALL = "all"
    SUM = "sum"
    PRODUCT = "product"
    COUNT = "count"
    MEAN = "mean"


class ConditionOperator(StrEnum):
    """Operators allowed in ``condition`` expressions."""

    EQUAL_TO = "equal_to"
    NOT_EQUAL_TO = "not_equal_to"
    CONTAINS = "contains"
    DOES_NOT_CONTAIN = "does_not_contain"
    LIST_CONTAINS = "list_contains"
    LIST_DOES_NOT_CONTAIN = "list_does_not_contain"
    LIST_CONTAINS_INTEGER = "list_contains_integer"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL_TO = "greater_than_or_equal_to"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL_TO = "less_than_or_equal_to"
    BOOLEAN_EQUAL_TO = "boolean_equal_to"
    FLOAT_EQUAL_TO = "float_equal_to"
    FLOAT_BETWEEN = "float_between"
    DATE_EQUAL_TO = "date_equal_to"
    DATE_BETWEEN = "date_between"
    BETWEEN = "between"
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"


_ALLOWED_FREQUENCY_OPERATORS = {
    member.value for member in FrequencyOperator.__members__.values()
}
_ALLOWED_CONDITION_OPERATORS = {
    member.value for member in ConditionOperator.__members__.values()
}


class _StrictModel(BaseModel):
    """Base strict model shared by query AST nodes."""

    model_config = ConfigDict(extra="forbid")


class DuringTheLast(_StrictModel):
    """Payload under ``during.the_last``."""

    unit: TimeUnit
    value: int


class During(_StrictModel):
    """Container for rolling window definitions."""

    the_last: DuringTheLast


class FrequencyCondition(RootModel["dict[str, Union[int, float]]"]):
    """Single-operator frequency expression."""

    @field_validator("root")
    @classmethod
    def _validate_frequency(
        cls, value: dict[str, Union[int, float]]
    ) -> dict[str, Union[int, float]]:
        if len(value) != 1:
            raise ValueError("must contain a single operator")
        operator = next(iter(value))
        if operator not in _ALLOWED_FREQUENCY_OPERATORS:
            raise ValueError(f"unsupported operator '{operator}'")
        return value


class ConditionExpression(RootModel["dict[str, ConditionValue]"]):
    """Single-operator condition expression."""

    @field_validator("root")
    @classmethod
    def _validate_condition(
        cls, value: dict[str, ConditionValue]
    ) -> dict[str, ConditionValue]:
        if len(value) != 1:
            raise ValueError("must contain a single operator")

        operator, operator_value = next(iter(value.items()))
        if operator not in _ALLOWED_CONDITION_OPERATORS:
            raise ValueError(f"unsupported operator '{operator}'")

        if operator == "between":
            if not isinstance(operator_value, dict):
                raise ValueError("between must be an object with start/end")
            if set(operator_value) != {"start", "end"}:
                raise ValueError("between must contain only start/end")

        return value


class FunctionConditionExpression(_StrictModel):
    """Functional condition with nested where clause."""

    condition: ConditionExpression
    function: WhereFunction
    property: str
    where: Union[PermutiveQueryNode, None] = None


class PropertyConditionNode(_StrictModel):
    """Node representing ``{condition, property}`` constraints."""

    condition: Union[
        ConditionExpression,
        FunctionConditionExpression,
        Literal["is_empty", "is_not_empty"],
    ]
    property: str


class InSegmentNode(_StrictModel):
    """Node for first-party segment membership checks."""

    in_segment: Union[str, int]


class TransitionPayload(_StrictModel):
    """Payload shared by transition nodes."""

    segment: Union[str, int]
    during: Union[During, None] = None


class HasEnteredNode(_StrictModel):
    """Node for ``has_entered`` transition checks."""

    has_entered: TransitionPayload


class HasNotEnteredNode(_StrictModel):
    """Node for ``has_not_entered`` transition checks."""

    has_not_entered: TransitionPayload


class HasExitedNode(_StrictModel):
    """Node for ``has_exited`` transition checks."""

    has_exited: TransitionPayload


class HasNotExitedNode(_StrictModel):
    """Node for ``has_not_exited`` transition checks."""

    has_not_exited: TransitionPayload


class SecondPartyPayload(_StrictModel):
    """Payload for second-party segment nodes."""

    provider: str
    segment: str

    @model_validator(mode="after")
    def _validate_second_party_payload(self) -> SecondPartyPayload:
        return self


class InSecondPartyNode(_StrictModel):
    """Node for ``in_second_party_segment`` checks."""

    in_second_party_segment: SecondPartyPayload


class NotInSecondPartyNode(_StrictModel):
    """Node for ``not_in_second_party_segment`` checks."""

    not_in_second_party_segment: SecondPartyPayload


class ConnectionsImportPayload(_StrictModel):
    """Payload for connections import membership nodes."""

    field_name: Union[str, None] = None
    value: Union[str, None] = None
    provider: Union[str, None] = None

    @model_validator(mode="after")
    def _validate_connections_import_payload(self) -> ConnectionsImportPayload:
        if self.provider:
            return self
        if self.field_name and self.value:
            return self
        raise ValueError(
            "must include either 'provider' or both 'field_name' and 'value'"
        )


class InConnectionsImportNode(_StrictModel):
    """Node for ``in_connections_import_segment`` checks."""

    in_connections_import_segment: ConnectionsImportPayload


class NotInConnectionsImportNode(_StrictModel):
    """Node for ``not_in_connections_import_segment`` checks."""

    not_in_connections_import_segment: ConnectionsImportPayload


class ThirdPartyPayload(_StrictModel):
    """Payload for third-party segment nodes."""

    segment: Union[str, int]
    provider: Union[str, None] = None


class InThirdPartyNode(_StrictModel):
    """Node for ``in_third_party_segment`` checks."""

    in_third_party_segment: ThirdPartyPayload


class NotInThirdPartyNode(_StrictModel):
    """Node for ``not_in_third_party_segment`` checks."""

    not_in_third_party_segment: ThirdPartyPayload


class EngagedTimePayload(_StrictModel):
    """Payload for ``engaged_time`` checks."""

    seconds: FrequencyCondition
    where: Union[PermutiveQueryNode, None] = None
    during: Union[During, Literal["this_session"], None] = None


class EngagedTimeNode(_StrictModel):
    """Node for ``engaged_time`` checks."""

    engaged_time: EngagedTimePayload


class EngagedCompletionPayload(_StrictModel):
    """Payload for ``engaged_completion`` checks."""

    completion: FrequencyCondition
    where: Union[PermutiveQueryNode, None] = None


class EngagedCompletionNode(_StrictModel):
    """Node for ``engaged_completion`` checks."""

    engaged_completion: EngagedCompletionPayload


class EngagedViewsPayload(_StrictModel):
    """Payload for ``engaged_views`` checks."""

    engaged_time: FrequencyCondition
    times: FrequencyCondition
    where: Union[PermutiveQueryNode, None] = None
    during: Union[During, Literal["this_session"], None] = None


class EngagedViewsNode(_StrictModel):
    """Node for ``engaged_views`` checks."""

    engaged_views: EngagedViewsPayload


class EventNode(_StrictModel):
    """Node representing event-based filters."""

    event: str
    frequency: FrequencyCondition
    where: Union[PermutiveQueryNode, None] = None
    during: Union[During, Literal["this_session"], None] = None


class FrequencyOnlyNode(_StrictModel):
    """Node representing standalone frequency constraints."""

    frequency: FrequencyCondition


class OrNode(_StrictModel):
    """Node representing logical OR."""

    or_: list[PermutiveQueryNode] = Field(alias="or")


class AndNode(_StrictModel):
    """Node representing logical AND."""

    and_: list[PermutiveQueryNode] = Field(alias="and")


PermutiveNodeModel: TypeAlias = Union[
    AndNode,
    OrNode,
    EventNode,
    FrequencyOnlyNode,
    InSegmentNode,
    HasEnteredNode,
    HasNotEnteredNode,
    HasExitedNode,
    HasNotExitedNode,
    InSecondPartyNode,
    NotInSecondPartyNode,
    InConnectionsImportNode,
    NotInConnectionsImportNode,
    InThirdPartyNode,
    NotInThirdPartyNode,
    EngagedTimeNode,
    EngagedCompletionNode,
    EngagedViewsNode,
    PropertyConditionNode,
]


class PermutiveQueryNode(RootModel[PermutiveNodeModel]):
    """Recursive wrapper around all supported query node shapes."""


FunctionConditionExpression.model_rebuild()
PropertyConditionNode.model_rebuild()
EngagedTimePayload.model_rebuild()
EngagedTimeNode.model_rebuild()
EngagedCompletionPayload.model_rebuild()
EngagedCompletionNode.model_rebuild()
EngagedViewsPayload.model_rebuild()
EngagedViewsNode.model_rebuild()
EventNode.model_rebuild()
OrNode.model_rebuild()
AndNode.model_rebuild()
PermutiveQueryNode.model_rebuild()


class PermutiveQueryASTStructure(BaseModel):
    """Pydantic structure for a native Permutive query payload.

    Methods
    -------
    _validate_ast(value)
        Validate payload with the recursive AST model.
    """

    query: PermutiveQueryNode = Field(
        description="Native Permutive cohort query payload parsed as typed AST."
    )

    @field_validator("query", mode="before")
    @classmethod
    def _validate_ast(cls, value: Any) -> PermutiveQueryNode:
        """Validate and coerce payload into ``PermutiveQueryNode``."""
        return PermutiveQueryNode.model_validate(value)


def validate_permutive_query_ast_json(payload: Union[str, dict[str, Any]]) -> bool:
    """Return whether payload matches the typed AST schema.

    Parameters
    ----------
    payload
        Query payload as JSON string or pre-parsed dictionary.

    Returns
    -------
    bool
        ``True`` when payload validates, otherwise ``False``.
    """
    try:
        candidate = json.loads(payload) if isinstance(payload, str) else payload
        if not isinstance(candidate, dict):
            return False
        PermutiveQueryASTStructure.model_validate({"query": candidate})
    except (TypeError, ValueError, ValidationError, json.JSONDecodeError):
        return False
    return True
