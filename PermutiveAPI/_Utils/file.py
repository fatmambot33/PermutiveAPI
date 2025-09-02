"""File and path utilities split from the legacy utils module.

Functions
---------
check_filepath(filepath)
    Ensure the parent directory of ``filepath`` exists, creating it if needed.
split_filepath(fullfilepath)
    Split a file path into directory, base name, and extension.
"""

from __future__ import annotations

import os
from typing import Tuple


def check_filepath(filepath: str) -> None:
    """Ensure the parent directory exists for a target file path.

    Parameters
    ----------
    filepath : str
        Absolute or relative path to the file to be written.
    """
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def split_filepath(fullfilepath: str) -> Tuple[str, str, str]:
    """Split a file path into directory, base name, and extension.

    Parameters
    ----------
    fullfilepath : str
        Absolute or relative file path.

    Returns
    -------
    tuple[str, str, str]
        A 3-tuple of ``(directory, name, extension)``.
    """
    path = os.path.dirname(fullfilepath)
    name, ext = os.path.splitext(os.path.basename(fullfilepath))
    return path, name, ext


__all__ = [
    "check_filepath",
    "split_filepath",
]
