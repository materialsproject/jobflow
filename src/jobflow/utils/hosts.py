"""Tools for managing hosts."""

from collections.abc import Sequence
from typing import TypeAlias

HostPairInput: TypeAlias = Sequence[str | int]

HostsInput: TypeAlias = HostPairInput | Sequence[HostPairInput]
#
#
# def normalize_hosts(hosts: HostsInput) -> list[tuple[str, int]]:
#     def to_pair(item: HostPairInput) -> tuple[str, int]:
#         a, b = item
#         if not isinstance(a, str) or not isinstance(b, int):
#             raise TypeError(f"Invalid host pair: {item}")
#         return (a, b)
#
#     # single pair: ("x",1) or ["x",1]
#     if (
#         isinstance(hosts, (list, tuple))
#         and len(hosts) == 2
#         and isinstance(hosts[0], str)
#         and isinstance(hosts[1], int)
#     ):
#         return [to_pair(hosts)]  # mypy is happy
#
#     # list of pairs
#     if isinstance(hosts, list):
#         return [to_pair(x) for x in hosts]
#
#     # fallback (shouldn't happen with declared types)
#     raise TypeError("Invalid hosts input")


def normalize_hosts(
    hosts: HostsInput,
) -> list[tuple[str, int]]:
    """
    Normalize various host formats into a list of (str, int) tuples.

    Parameters
    ----------
    hosts
        The hosts to be normalized.

    Returns
    -------
    A list of (str, int) tuples
    """
    if not hosts:
        return []

    # If it's a single tuple, wrap it in a list
    if isinstance(hosts, tuple):
        return [hosts]

    if isinstance(hosts, list):
        first_item = hosts[0]

        # Single host as flat list: [str, int]
        if isinstance(first_item, str):
            return [(hosts[0], hosts[1])]  # type: ignore

        # List of tuples or lists: [(str, int), ...] or [[str, int], ...]
        if isinstance(first_item, (tuple, list)):
            return [tuple(item) if isinstance(item, list) else item for item in hosts]  # type: ignore

    raise TypeError(f"Unsupported type: {type(hosts)}")
