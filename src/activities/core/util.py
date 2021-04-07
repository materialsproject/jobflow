import logging
from enum import Enum
from typing import Any, Dict, Hashable, List, Tuple, Type, Union

from monty.json import MSONable, jsanitize


class ValueEnum(Enum):
    """
    Enum that serializes to string as the value and can be compared against a string.
    """

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        if type(self) == type(other) and self.value == other.value:
            return True
        else:
            return str(self.value) == str(other)


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
    (('a', 1), ('c', 'd'))
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


def update_in_dictionary(d: Dict[Hashable, Any], updates: Dict[Tuple, Any]):
    """
    Update a dictionary (in place) at specific locations with a new values.

    This function works on nested dictionaries and those containing lists or tuples.

    For example:

    ```python
    d = {
        "a": [0, {"b": 1, "x": 2}],
        "c": {
            "d": {"x": 3}
        }
    }
    update_in_dictionary(d, {('a', 1, 'x'): 100, ('c', 'd', 'x'): 100})

    # d = {
    #     "a": [0, {"b": 1, "x": 100}],
    #     "c": {
    #         "d": {"x": 100}
    #     }
    # }
    ```

    Args:
        d: A dictionary to update.
        updates: The updates to perform, as a dictionary of {location: update}.
    """
    for loc, update in updates.items():
        pos = d
        for idx in loc[:-1]:
            pos = pos[idx]
        pos[loc[-1]] = update


def initialize_logger(level: int = logging.INFO) -> logging.Logger:
    """Initialize the default logger.

    Parameters
    ----------
    level
        The log level.

    Returns
    -------
    A logging instance with customized formatter and handlers.
    """
    import sys

    log = logging.getLogger("activities")
    log.setLevel(level)
    log.handlers = []  # reset logging handlers if they already exist

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(fmt)
    log.addHandler(screen_handler)
    return log


def contains_activity_or_job(arg: Any) -> bool:
    from activities.core.activity import Activity
    from activities.core.job import Job

    if isinstance(arg, (Activity, Job)):
        # if the argument is an activity or job then stop there
        return True

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find an activity or job here
        return False

    arg = jsanitize(arg, strict=True)

    # recursively find any reference classes
    locations = find_key_value(arg, "@class", "Activity")
    locations += find_key_value(arg, "@class", "Job")

    return len(locations) > 0


def suuid() -> str:
    from uuid import uuid4

    return str(uuid4())
