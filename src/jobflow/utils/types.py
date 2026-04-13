"""Utilities for types."""

from __future__ import annotations

from typing import Any


def lenient_issubclass(cls: Any, class_or_tuple: Any) -> bool:
    """
    Check if a class is a subclass of another class.

    Partially inspired by pydantic.v1.utils.lenient_issubclass.
    TypeError is not raised if the standard issublass fails.

    Parameters
    ----------
    cls
        The class to check.
    class_or_tuple
        The potential parent class of the class to check.

    Returns
    -------
    bool
        True if the class is a subclass of the target class.
    """
    try:
        return isinstance(cls, type) and issubclass(cls, class_or_tuple)
    except TypeError:
        return False
