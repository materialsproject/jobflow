"""Tools for generating UUIDs."""

from __future__ import annotations

from uuid import UUID

try:  # pragma: no cover
    from ulid import ULID
except ImportError:  # pragma: no cover
    err_msg = (
        "The ulid package is not installed. "
        "Install it with `pip install jobflow[ulid]` or `pip install python-ulid`."
    )

    class ULID:  # type: ignore[no-redef]
        """Fake ULID class for raising import error."""

        def __init__(self, *args, **kwargs):
            raise ImportError(err_msg)

        def from_str(self, *args, **kwargs):
            """Raise import error."""
            raise ImportError(err_msg)


def suid(id_type: str | None = None) -> str:
    """Generate a string UUID (universally unique identifier).

    Since the timestamp of the IDs are important for sorting,
    only id types that include a timestamp are supported.

    Parameters
    ----------
    uuid_type:
        The type of UUID to generate.
        In the future, ``uuid7`` and ``ulid`` may be supported.

    Returns
    -------
    str
        A UUID.
    """
    import uuid

    from jobflow import SETTINGS

    if id_type is None:
        id_type = SETTINGS.UID_TYPE

    funcs = {
        "uuid1": uuid.uuid1,
        "uuid4": uuid.uuid4,
        "ulid": ULID,
    }
    if id_type not in funcs:
        raise ValueError(f"UUID type {id_type} not supported.")
    return str(funcs[id_type]())


def get_timestamp_from_uid(uid: str) -> float:
    """
    Get the time that a UID was generated.

    Parameters
    ----------
    uuid
        A UUID.

    Returns
    -------
    float
        The time stamp from the UUID.
    """
    id_type = _get_id_type(uid)
    if id_type == "uuid4":
        raise ValueError(
            "UUID4 is randomly generated and not associated with a time stamp."
        )
    funcs = {
        "uuid1": lambda uuid: (UUID(uuid).time - 0x01B21DD213814000) / 1e7,
        "ulid": lambda uuid: ULID.from_str(uuid).timestamp,
    }
    return funcs[id_type](uid)


def _get_id_type(uid: str) -> str:
    """
    Get the type of a UUID.

    Parameters
    ----------
    uuid
        A UUID.

    Returns
    -------
    str
        The type of the UUID.
    """
    try:
        version = UUID(uid).version
        return {
            1: "uuid1",
            4: "uuid4",
        }[version]
    except ValueError:
        pass

    try:
        ULID.from_str(uid)
        return "ulid"
    except ValueError:
        pass

    raise ValueError(f"ID type for {uid} not recognized.")
