"""
Implementation of the DictMod language for manipulating dictionaries.

This module enables the modification of a dict using another dict. The main method of
interest is :obj:`apply_mod`.

.. Note::
    This code is based heavily on the Ansible class of `custodian
    <https://pypi.python.org/pypi/custodian>`_, but simplifies it considerably for the
    limited use cases required by jobflow.

    The original version of this file was written by Shyue Ping Ong and Anubhav Jain.
"""

from __future__ import annotations

import re
import typing

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Optional, Tuple


__all__ = ["DictMods", "apply_mod"]


class DictMods:
    """
    Class to define mongo-like modifications on a dict.

    Supported keywords include the following Mongo-based keywords, with the usual
    meanings (refer to Mongo documentation for information):

    - ``_inc``
    - ``_set``
    - ``_unset``
    - ``_push``
    - ``_push_all``
    - ``_add_to_set`` (but ``_each`` is not supported)
    - ``_pop``
    - ``_pull``
    - ``_pull_all``
    - ``_rename``

    .. Note::

        Note that ``_set`` does not support modification of nested dicts using the mongo
        ``{"a.b":1}`` notation. This is because mongo does not allow keys with "." to be
        inserted. Instead, nested dict modification is supported using a special "->"
        keyword, e.g. ``{"a->b": 1}``
    """

    def __init__(self):
        self.supported_actions = {}
        for i in dir(self):
            if (not re.match(r"__\w+__", i)) and callable(getattr(self, i)):
                self.supported_actions["_" + i] = getattr(self, i)

    @staticmethod
    def set(input_dict, settings):
        """Set a value."""
        for k, v in settings.items():
            (d, key) = _get_nested_dict(input_dict, k)
            d[key] = v

    @staticmethod
    def unset(input_dict, settings):
        """Unset a value."""
        for k in settings.keys():
            (d, key) = _get_nested_dict(input_dict, k)
            del d[key]

    @staticmethod
    def push(input_dict, settings):
        """Append a value to a list or create a list it if it doesn't exist."""
        for k, v in settings.items():
            (d, key) = _get_nested_dict(input_dict, k)
            if key in d:
                d[key].append(v)
            else:
                d[key] = [v]

    @staticmethod
    def push_all(input_dict, settings):
        """Extend a list or create a list it if it doesn't exist."""
        for k, v in settings.items():
            (d, key) = _get_nested_dict(input_dict, k)
            if key in d:
                d[key].extend(v)
            else:
                d[key] = v

    @staticmethod
    def inc(input_dict, settings):
        """Increment a number."""
        for k, v in settings.items():
            (d, key) = _get_nested_dict(input_dict, k)
            if key in d:
                d[key] += v
            else:
                d[key] = v

    @staticmethod
    def rename(input_dict, settings):
        """Rename a key."""
        for k, v in settings.items():
            if k in input_dict:
                input_dict[v] = input_dict[k]
                del input_dict[k]

    @staticmethod
    def add_to_set(input_dict, settings):
        """Add an item to a set or create the set if it doesn't exist."""
        for k, v in settings.items():
            (d, key) = _get_nested_dict(input_dict, k)
            if key in d and (not isinstance(d[key], (list, tuple))):
                raise ValueError(f"Keyword {k} does not refer to an array.")
            if key in d and v not in d[key]:
                d[key].append(v)
            elif key not in d:
                d[key] = v

    @staticmethod
    def pull(input_dict, settings):
        """Extract a field from a nested dictionary."""
        for k, v in settings.items():
            (d, key) = _get_nested_dict(input_dict, k)
            if key in d and (not isinstance(d[key], (list, tuple))):
                raise ValueError(f"Keyword {k} does not refer to an array.")
            if key in d:
                d[key] = [i for i in d[key] if i != v]

    @staticmethod
    def pull_all(input_dict, settings):
        """Extract many fields from a nested dictionary."""
        for k, v in settings.items():
            if k in input_dict and (not isinstance(input_dict[k], (list, tuple))):
                raise ValueError(f"Keyword {k} does not refer to an array.")
            for i in v:
                DictMods.pull(input_dict, {k: i})

    @staticmethod
    def pop(input_dict, settings):
        """Pop a value from a list."""
        for k, v in settings.items():
            (d, key) = _get_nested_dict(input_dict, k)
            if key in d and (not isinstance(d[key], (list, tuple))):
                raise ValueError(f"Keyword {k} does not refer to an array.")
            if v == 1:
                d[key].pop()
            elif v == -1:
                d[key].pop(0)


_DM = DictMods()


def apply_mod(modification: Dict[str, Any], obj: Dict[str, Any]):
    """
    Apply a dict mod to an object.

    Note that modify makes actual in-place modifications. It does not return a copy.

    Parameters
    ----------
    modification
        Modification must be ``{action_keyword : settings}``, where action_keyword is a
        supported DictMod.
    obj
        A dict to be modified.
    """
    for action, settings in modification.items():
        if action in _DM.supported_actions:
            _DM.supported_actions[action].__call__(obj, settings)
        else:
            raise ValueError(f"{action} is not a supported action!")


def _get_nested_dict(
    input_dict: Dict[str, Any], key: str
) -> Optional[Tuple[Dict[str, Any], str]]:
    """Get nested dicts using a key."""
    current = input_dict
    toks = [t for t in key.split("->") if t != ""]
    n = len(toks)
    for i, tok in enumerate(toks):
        if tok not in current and i < n - 1:
            current[tok] = {}
        elif i == n - 1:
            return current, toks[-1]
        current = current[tok]
    return None
