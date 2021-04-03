import logging
from enum import Enum
from typing import Any, Dict, Hashable, List, Tuple, Union


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
    d: Union[Dict[Hashable, Any], List[Any]], key: Hashable
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

    Returns
    -------
    A tuple of routes to where the matches were found.

    Examples
    --------
    >>> data = {
    ...    "a": [0, {"b": 1, "x": 3}],
    ...    "c": {"d": {"x": 3}}
    ... }
    >>> find_key(data, "x")
    (('a', 1), ('c', 'd'))
    """
    found_items = set()

    def _lookup(obj, path=None):
        if path is None:
            path = ()

        if isinstance(obj, dict):
            if key in obj:
                found_items.add(path)

            for k, v in obj.items():
                _lookup(v, path + (k,))

        elif isinstance(obj, (list, tuple)):
            for i, v in enumerate(obj):
                _lookup(v, path + (i,))

    _lookup(d)
    return tuple([list(path) for path in found_items])


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


def find_in_dictionary(
    d: Dict[Hashable, Any], keys: Union[Hashable, List[Hashable]]
) -> Dict[Tuple, Any]:
    """
    Find the route to and values of keys in a dictionary.

    This function works on nested dictionaries and those containing lists or tuples.

    For example:

    ```python
    d = {
        "a": [0, {"b": 1, "x": 2}],
        "c": {
            "d": {"x": 3}
        }
    }
    find_in_dictionary(d, ["b", "x"])

    # returns: {('a', 1, 'x'): 2, ('a', 1, 'b'): 1, ('c', 'd', 'x'): 3}
    ```

    Args:
        d: A dictionary.
        keys: A key or list of keys to find.

    Returns:
        A dictionary mapping the route to the keys and the value at that route.
    """
    if not isinstance(keys, list):
        keys = [keys]
    found_items = {}

    def _lookup(obj, path=None):
        if path is None:
            path = ()

        if isinstance(obj, dict):
            for key in keys:
                if key in obj:
                    found_items[path + (key,)] = obj[key]

            for k, v in obj.items():
                _lookup(v, path + (k,))

        elif isinstance(obj, (list, tuple)):
            for i, v in enumerate(obj):
                _lookup(v, path + (i,))

    _lookup(d)
    return found_items


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
