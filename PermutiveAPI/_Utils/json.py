"""JSON utilities split from the legacy utils module.

This module centralizes JSON serialization and deserialization helpers
used across the codebase. It mirrors the functionality present in the
legacy `PermutiveAPI/Utils.py` so we can migrate incrementally.

Functions
---------
json_default(value)
    Provide JSON serialization for complex data types.
load_json_list(data, list_name, item_name=None)
    Load a list of dictionaries from JSON representations.

Classes
-------
customJSONEncoder
    Custom JSON encoder delegating to ``json_default``.
JSONSerializable
    Mixin that provides ``to_json``/``from_json`` helpers for objects.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
import datetime
from dataclasses import is_dataclass, fields
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from typing import get_args, get_origin


def to_payload(
    dataclass_obj: Any, api_payload: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Convert a dataclass-like object to a JSON-ready dict.

    Fields with ``None`` values are omitted. If ``api_payload`` is provided,
    only keys included in this list are kept. Values are serialized using
    ``customJSONEncoder`` to ensure complex types are handled.

    Parameters
    ----------
    dataclass_obj : Any
        The dataclass instance to convert into a payload.
    api_payload : list[str] | None, optional
        A specific list of keys to include in the payload. If ``None``, all
        non-None fields are included. Defaults to ``None``.

    Returns
    -------
    Dict[str, Any]
        A JSON-serializable dictionary representing the payload.
    """
    dataclass_dict = vars(dataclass_obj)
    filtered_dict = {
        k: v
        for k, v in dataclass_dict.items()
        if v is not None and (api_payload is None or k in api_payload)
    }
    return cast(
        Dict[str, Any], json.loads(json.dumps(filtered_dict, cls=customJSONEncoder))
    )


def load_json_list(
    data: Union[dict, List[dict], str, Path],
    list_name: str,
    item_name: Optional[str] = None,
) -> List[dict]:
    """Load a list of dictionaries from various JSON representations.

    Parameters
    ----------
    data : Union[dict, List[dict], str, Path]
        The JSON data to deserialize. It can be a dictionary, a list of
        dictionaries, a JSON string, or a path to a JSON file.
    list_name : str
        Name of the list class for error messages.
    item_name : str | None, optional
        Name of the item class for error messages. Defaults to the list name
        with ``'List'`` trimmed.

    Returns
    -------
    list[dict]
        The parsed list of dictionaries.

    Raises
    ------
    TypeError
        If ``data`` cannot be converted to a list of dictionaries.
    """
    if item_name is None and list_name.endswith("List"):
        item_name = list_name[:-4]

    if isinstance(data, dict):
        raise TypeError(
            (
                "Cannot create a {list_name} from a dictionary. "
                "Use from_json on the {item_name} class for single objects."
            ).format(list_name=list_name, item_name=item_name or "item")
        )

    if isinstance(data, (str, Path)):
        try:
            content = (
                data.read_text(encoding="utf-8") if isinstance(data, Path) else data
            )
            loaded = json.loads(content)
        except Exception as exc:  # pragma: no cover - error path
            raise TypeError(f"Failed to parse JSON from input: {exc}")
        if not isinstance(loaded, list):
            raise TypeError(
                ("JSON content from {kind} did not decode to a list.").format(
                    kind=type(data).__name__
                )
            )
        data = loaded

    if isinstance(data, list):
        return data

    raise TypeError(
        (
            "`from_json()` expected a list of dicts, JSON string, or Path, "
            "but got {kind}"
        ).format(kind=type(data).__name__)
    )


T = TypeVar("T", bound="JSONSerializable")
JSONOutput = TypeVar("JSONOutput", Dict[str, Any], List[Any])


def json_default(value: Any):
    """Provide JSON serialization for complex data types.

    Parameters
    ----------
    value : Any
        Value to serialize.

    Returns
    -------
    Any
        JSON-compatible value.
    """
    if isinstance(value, Enum):
        return value.value
    elif isinstance(value, (float, Decimal)):
        return float(value)
    elif isinstance(value, (int)):
        return int(value)
    elif isinstance(value, uuid.UUID):
        return str(value)
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, datetime.date):
        return {"year": value.year, "month": value.month, "day": value.day}
    elif isinstance(value, (list, set, tuple)):
        return [json_default(item) for item in value]
    elif isinstance(value, dict):
        return {k: json_default(v) for k, v in value.items()}
    elif hasattr(value, "__dict__"):
        return {k: json_default(v) for k, v in value.__dict__.items()}
    elif value is None:
        return None
    else:
        return str(value)


class customJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for complex data types.

    Methods
    -------
    default(o)
        Override default encoder to handle complex types via ``json_default``.
    """

    def default(self, o):
        """Override the default JSON encoder to handle complex data types."""
        try:
            return json_default(o)
        except TypeError:
            return super().default(o)


class JSONSerializable(Generic[JSONOutput]):
    """Mixin providing JSON serialization and deserialization capabilities.

    This is a generic mixin that should be used with a type argument specifying
    the output of ``to_json``, e.g., ``JSONSerializable[Dict[str, Any]]``.

    Methods
    -------
    __str__() -> str
        Pretty-print JSON when calling ``print(object)``.
    __repr__() -> str
        Return a concise representation highlighting key fields.
    to_json() -> JSONOutput
        Convert the object to a JSON-serializable format.
    from_json(cls, data: dict) -> T
        Create an instance of the class from a JSON dictionary.
    to_json_file(filepath: str | Path)
        Serialize the object to a JSON file using ``customJSONEncoder``.
    from_json_file(cls, filepath: str | Path) -> T
        Create an instance of the class from a JSON file.
    """

    def __str__(self) -> str:
        """Return a human-readable JSON representation.

        Returns
        -------
        str
            Pretty-printed JSON for the instance using ``customJSONEncoder``.
        """
        return json.dumps(self.to_json(), indent=4, cls=customJSONEncoder)

    def __repr__(self) -> str:
        """Return a concise developer-friendly representation.

        The representation prefers key identity fields when available
        (e.g., ``name``, ``id``, ``code``) and otherwise summarises content.

        Returns
        -------
        str
            A concise, informative string describing this instance.
        """
        tname = type(self).__name__

        # List-like containers: summarise by length to avoid verbose output
        if isinstance(self, list):
            return f"{tname}(n={len(self)})"

        # Dict-like containers: summarise by number of keys
        if isinstance(self, dict):
            return f"{tname}(keys={len(self)})"

        # Dataclasses: prefer common identity fields when present
        if is_dataclass(self):
            try:
                values: Dict[str, Any] = {
                    f.name: getattr(self, f.name) for f in fields(self)
                }
            except Exception:
                values = {}

            parts: List[str] = []
            for key in ("name", "id", "code"):
                if key in values and values[key] not in (None, ""):
                    parts.append(f"{key}={values[key]!r}")

            # If none of the identity fields exist, include up to three simple fields
            if not parts:
                simple_keys = [
                    k
                    for k, v in values.items()
                    if k not in ("created_at", "updated_at", "last_updated_at")
                    and not k.startswith("_")
                    and isinstance(v, (str, int, float, bool))
                    and v not in (None, "")
                ][:3]
                parts = [f"{k}={values[k]!r}" for k in simple_keys]

            return f"{tname}({', '.join(parts)})" if parts else f"{tname}()"

        # Fallback to attribute dict summary for other objects
        if hasattr(self, "__dict__"):
            public = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
            keys = [
                k for k, v in public.items() if isinstance(v, (str, int, float, bool))
            ][:3]
            parts = [f"{k}={public[k]!r}" for k in keys]
            return f"{tname}({', '.join(parts)})" if parts else f"{tname}()"

        return f"{tname}()"

    def to_json(self) -> JSONOutput:
        """Convert the object to a JSON-serializable format.

        Returns
        -------
        JSONOutput
            JSON-serializable representation of the object.
        """
        if isinstance(self, dict):
            return cast(
                JSONOutput,
                {
                    k: JSONSerializable.serialize_value(v)
                    for k, v in self.items()
                    if not str(k).startswith("_")
                },
            )
        elif isinstance(self, list):
            return cast(
                JSONOutput,
                [
                    JSONSerializable.serialize_value(item)
                    for item in self
                    if item not in (None, [], {})
                ],
            )
        elif is_dataclass(self):
            result: Dict[str, Any] = {}
            for f in fields(self):
                try:
                    value = getattr(self, f.name)
                    serialized = JSONSerializable.serialize_value(value)
                    if serialized not in (None, [], {}):
                        result[f.name] = serialized
                except Exception as e:
                    logging.warning(f"Error serializing field {f.name}: {e}")
            return cast(JSONOutput, result)
        elif hasattr(self, "__dict__"):
            return cast(
                JSONOutput,
                {
                    k: JSONSerializable.serialize_value(v)
                    for k, v in self.__dict__.items()
                    if not k.startswith("_")
                },
            )
        raise TypeError(f"{type(self).__name__} is not JSON-serializable")

    @staticmethod
    def serialize_value(
        value: Any,
    ) -> Union[Dict[str, Any], List[Any], str, int, float, None]:
        """Convert a Python value into a JSON-compatible representation.

        Parameters
        ----------
        value : Any
            The Python value to convert.

        Returns
        -------
        dict | list | str | int | float | None
            A JSON-serializable value suitable for dumping.
        """
        if isinstance(value, JSONSerializable):
            return value.to_json()
        if isinstance(value, list):
            return [
                JSONSerializable.serialize_value(item)
                for item in value
                if item not in (None, [], {})
            ]
        if isinstance(value, dict):
            return {
                k: JSONSerializable.serialize_value(v)
                for k, v in value.items()
                if v not in (None, [], {})
            }
        try:
            return json_default(value)
        except Exception:
            return value  # Fallback to raw value

    def to_json_file(self, filepath: str | Path) -> None:
        """Write the JSON representation to a file.

        Parameters
        ----------
        filepath : str or Path
            Destination file path.
        """
        with open(Path(filepath), "w", encoding="utf-8") as f:
            json.dump(self.to_json(), f, indent=4, cls=customJSONEncoder)

    @classmethod
    def from_json_file(cls: type[T], filepath: str | Path) -> T:
        """Create an instance from a JSON file.

        Parameters
        ----------
        filepath : str or Path
            Path to a JSON file.

        Returns
        -------
        T
            An instance of the class.
        """
        p = Path(filepath)
        return cls.from_json(p.read_text(encoding="utf-8"))

    @classmethod
    def from_json(cls: type[T], data: dict | str | Path) -> T:
        """Create an instance of ``cls`` from JSON-like input.

        Parameters
        ----------
        data : Union[dict, str, Path]
            The JSON data to deserialize. It can be a dictionary, a JSON
            string, or a path to a JSON file.

        Returns
        -------
        T
            An instance of the class.

        Raises
        ------
        TypeError
            If the input data is not a dict, string, or Path, or if parsing fails.
        """
        if isinstance(data, (str, Path)):
            try:
                content = (
                    data.read_text(encoding="utf-8") if isinstance(data, Path) else data
                )
                data = json.loads(content)
            except Exception as e:
                raise TypeError(f"Failed to parse JSON from input: {e}")

        if isinstance(data, dict):
            if is_dataclass(cls):
                module = sys.modules.get(cls.__module__)
                kwargs: Dict[str, Any] = {}
                for f in fields(cls):
                    if f.name in data:
                        kwargs[f.name] = JSONSerializable.unserialize_value(
                            data[f.name], f.type, module
                        )
                return cls(**kwargs)
            return cls(**data)

        raise TypeError(
            f"`from_json()` expected a dict, JSON string, or Path, but got {type(data).__name__}"
        )

    @staticmethod
    def unserialize_value(
        value: Any, annotation: Any, module: Optional[Any] = None
    ) -> Any:
        """Convert a JSON value into the annotated Python type.

        Parameters
        ----------
        value : Any
            The raw JSON value to convert.
        annotation : Any
            The target type annotation (e.g., ``datetime``, ``Optional[datetime]``,
            ``List[Alias]``, or a ``JSONSerializable`` subclass).
        module : Any | None, optional
            Module used to resolve forward-referenced annotations written as
            strings. Defaults to ``None``.

        Returns
        -------
        Any
            The converted value when a conversion is applicable, otherwise the
            original value.
        """
        if isinstance(annotation, str) and module is not None:
            annotation = getattr(module, annotation, annotation)

        def is_datetime_type(tp: Any) -> bool:
            if tp is datetime.datetime:
                return True
            origin = get_origin(tp)
            if origin is Union:
                return any(arg is datetime.datetime for arg in get_args(tp))
            return False

        def parse_iso_datetime(val: Any) -> Any:
            if isinstance(val, str):
                try:
                    iso = val.replace("Z", "+00:00")
                    return datetime.datetime.fromisoformat(iso)
                except Exception:
                    return val
            return val

        def is_jsonserializable_subclass(tp: Any) -> bool:
            try:
                return isinstance(tp, type) and issubclass(tp, JSONSerializable)
            except Exception:
                return False

        def is_classinfo(obj: Any) -> bool:
            if isinstance(obj, type):
                return True
            if isinstance(obj, tuple):
                return all(isinstance(x, type) for x in obj)
            return False

        if is_classinfo(annotation):
            classinfo = cast(Union[type, Tuple[type, ...]], annotation)
            if isinstance(value, classinfo):
                return value

        if is_datetime_type(annotation):
            return parse_iso_datetime(value)

        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin in (list, List) and isinstance(value, list) and args:
            return [
                JSONSerializable.unserialize_value(v, args[0], module) for v in value
            ]

        if origin in (dict, Dict) and isinstance(value, dict):
            return value

        if is_jsonserializable_subclass(annotation) and isinstance(value, dict):
            annot_cls = cast(Any, annotation)
            return annot_cls.from_json(value)

        return value
