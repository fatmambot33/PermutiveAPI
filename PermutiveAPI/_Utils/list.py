"""List utilities split from the legacy utils module.

Functions
---------
chunk_list(lst, n)
    Split a list into chunks of size ``n``.
convert_list(val)
    Convert a JSON-like string or list into a Python list.
compare_list(list1, list2)
    Compare two lists for set equality (order-insensitive).
merge_list(lst1, lst2=None)
    Merge two lists, remove duplicates, and return a sorted list.
"""

from __future__ import annotations

import ast
from typing import Any, List, Optional, TypeVar, Union

T_co = TypeVar("T_co")


def chunk_list(lst: List[T_co], n: int) -> List[List[T_co]]:
    """Split a list into chunks of size ``n``.

    Parameters
    ----------
    lst : list[T_co]
        The list to split.
    n : int
        Chunk size greater than zero.

    Returns
    -------
    list[list[T_co]]
        List of chunks in original order.
    """
    if n <= 0:
        raise ValueError("Chunk size 'n' must be greater than 0")
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def convert_list(val: Union[str, List[Any]]) -> List[Any]:
    """Convert a JSON-like string or list into a Python list.

    Parameters
    ----------
    val : str | list[Any]
        A string representation of a list (e.g., "[1, 2]") or a list.

    Returns
    -------
    list[Any]
        Parsed list.
    """
    if isinstance(val, list):
        return val
    try:
        parsed = ast.literal_eval(val)
    except Exception as exc:  # pragma: no cover - error path
        raise ValueError(f"Invalid list string: {exc}")
    if not isinstance(parsed, list):
        raise ValueError("String did not evaluate to a list")
    return parsed


def compare_list(list1: List[str], list2: List[str]) -> bool:
    """Compare two lists for set equality (order-insensitive).

    Parameters
    ----------
    list1 : list[str]
        The first list to compare.
    list2 : list[str]
        The second list to compare.

    Returns
    -------
    bool
        ``True`` if both lists contain the same elements, regardless of
        order, otherwise ``False``.
    """
    return set(list1) == set(list2)


def merge_list(lst1: List, lst2: Optional[Union[int, str, List]] = None) -> List:
    """Merge two lists, removing duplicates and sorting the result.

    Parameters
    ----------
    lst1 : list
        The first list.
    lst2 : int | str | list | None, optional
        The second list. If an int/str is passed, it is wrapped in a list.

    Returns
    -------
    list
        Sorted merged list without duplicates or None values.
    """
    if lst2 is None:
        lst2 = []
    elif isinstance(lst2, (str, int)):
        lst2 = [lst2]

    merged_set = set(lst1) | set(lst2)
    merged_set.discard(None)
    return sorted(list(merged_set))


__all__ = [
    "chunk_list",
    "convert_list",
    "compare_list",
    "merge_list",
]
