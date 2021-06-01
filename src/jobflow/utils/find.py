"""Tools for finding and replacing in dictionaries and other objects."""

from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Hashable, List, Tuple, Type, Union

    from monty.json import MSONable

__all__ = [
    "find_key",
    "find_key_value",
    "update_in_dictionary",
    "contains_flow_or_job",
]


def find_key(
    d: Union[Dict[Hashable, Any], List[Any]],
    key: Union[Hashable, Type[MSONable]],
    include_end: bool = False,
    nested: bool = False,
) -> List[List[Any]]:
    """
    Find the route to key: value pairs in a dictionary.

    This function works on nested dictionaries, lists, and tuples.

    Parameters
    ----------
    d
        A dict or list of dicts.
    key
        A dictionary key or MSONable class to locate.
    include_end
        Whether to include the key in the route. This has no effect if the key is an
        MSONable class.
    nested:
        Whether to return nested keys or stop at the first match.

    Returns
    -------
    A list of routes to where the matches were found.

    Examples
    --------
    >>> data = {
    ...    "a": [0, {"b": 1, "x": 3}],
    ...    "c": {"d": {"x": 3}}
    ... }
    >>> find_key(data, "x")
    [['a', 1], ['c', 'd']]
    >>> find_key(data, "x", include_end=True)
    [['a', 1, 'x'], ['c', 'd', 'x']]

    The ``nested`` argument can be used to control the behaviour of nested keys.

    >>> data = {"a": {"x": {"x": 1}}, "b": {"x": 0}}
    >>> find_key(data, "x", nested=False)
    [['a'], ['b']]
    >>> find_key(data, "x", nested=True)
    [['a'], ['a', 'x'], ['b']]
    """
    import inspect

    from monty.json import MSONable

    found_items = set()

    def _lookup(obj, path=None):
        found = False
        if path is None:
            path = ()

        if isinstance(obj, dict):
            if (
                inspect.isclass(key)
                and issubclass(key, MSONable)
                and "@module" in obj
                and obj["@module"] == key.__module__
                and "@class" in obj
                and obj["@class"] == key.__name__
            ):
                found_items.add(path)
                found = True

            if key in obj:
                if include_end:
                    found_items.add(path + (key,))
                else:
                    found_items.add(path)
                found = True

            if not found or nested:
                for k, v in obj.items():
                    _lookup(v, path + (k,))

        elif isinstance(obj, (list, tuple)):
            for i, v in enumerate(obj):
                _lookup(v, path + (i,))

    _lookup(d)
    return [list(path) for path in found_items]


def find_key_value(
    d: Union[Dict[Hashable, Any], List[Any]], key: Hashable, value: Hashable
) -> Tuple[List[Any], ...]:
    """
    Find the route to key: value pairs in a dictionary.

    This function works on nested dictionaries, lists, and tuples.

    Parameters
    ----------
    d
        A dict or list of dicts.
    key
        A dictionary key.
    value
        A value.

    Returns
    -------
    A tuple of routes to where the matches were found.

    Examples
    --------
    >>> data = {
    ...    "a": [0, {"b": 1, "x": 3}],
    ...    "c": {"d": {"x": 3}}
    ... }
    ... find_key_value(data, "x", 3)
    (['a', 1], ['c', 'd'])
    """
    found_items = set()

    def _lookup(obj, path=None):
        if path is None:
            path = ()

        if isinstance(obj, dict):
            if key in obj and obj[key] == value:
                found_items.add(path)

            for k, v in obj.items():
                _lookup(v, path + (k,))

        elif isinstance(obj, (list, tuple)):
            for i, v in enumerate(obj):
                _lookup(v, path + (i,))

    _lookup(d)
    return tuple([list(path) for path in found_items])


def update_in_dictionary(obj: Dict[Hashable, Any], updates: Dict[Tuple, Any]):
    """
    Update a dictionary in place at specific locations with a new values.

    This function works on nested dictionaries and those containing lists or tuples.

    Parameters
    ----------
    obj
        A dictionary to update.
    updates
        The updates to perform, as a dictionary of ``{location: update}``.

    Examples
    --------
    >>> data = {
    ...    "a": [0, {"b": 1, "x": 3}],
    ...    "c": {"d": {"x": 3}}
    ... }
    >>> update_in_dictionary(data, {('a', 1, 'x'): 100, ('c', 'd', 'x'): 100})
    >>> data
    {'a': [0, {'b': 1, 'x': 100}], 'c': {'d': {'x': 100}}}
    """
    for loc, update in updates.items():
        pos = obj
        for idx in loc[:-1]:
            pos = pos[idx]
        pos[loc[-1]] = update


def contains_flow_or_job(obj: Any) -> bool:
    """
    Find whether an object contains any :obj:`Flow` or :obj:`Job` objects.

    Parameters
    ----------
    obj
        An object.

    Returns
    -------
    bool
        Whether the object contains any Flows or jobs.
    """
    from monty.json import jsanitize

    from jobflow.core.flow import Flow
    from jobflow.core.job import Job

    if isinstance(obj, (Flow, Job)):
        # if the argument is an flow or job then stop there
        return True

    elif isinstance(obj, (float, int, str, bool)):
        # argument is a primitive, we won't find an flow or job here
        return False

    obj = jsanitize(obj, strict=True)

    # recursively find any reference classes
    locations = find_key_value(obj, "@class", "Flow")
    locations += find_key_value(obj, "@class", "Job")

    return len(locations) > 0
