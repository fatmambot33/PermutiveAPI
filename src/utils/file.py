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
from pathlib import Path
from typing import Tuple


def check_filepath(filepath: str | Path) -> None:
    """Ensure the parent directory exists for a target file path.

    Parameters
    ----------
    filepath : str or Path
        Absolute or relative path to the file to be written.
    """
    path_str = os.fspath(filepath)
    directory = os.path.dirname(path_str)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def split_filepath(fullfilepath: str | Path) -> Tuple[str, str, str]:
    """Split a file path into directory, base name, and extension.

    Parameters
    ----------
    fullfilepath : str or Path
        Absolute or relative file path.

    Returns
    -------
    tuple[str, str, str]
        A 3-tuple of ``(directory, name, extension)``.
    """
    path_str = os.fspath(fullfilepath)
    path = os.path.dirname(path_str)
    name, ext = os.path.splitext(os.path.basename(path_str))
    return path, name, ext


__all__ = [
    "check_filepath",
    "split_filepath",
]
