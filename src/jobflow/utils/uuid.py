"""Tools for generating UUIDs."""

from uuid import UUID


def suuid(id_type: str = "uuid1") -> str:
    """Generate a string UUID (universally unique identifier).

    Since the timestamp of the IDs are important for sorting,
    only id types that include a timestamp are supported.

    Parameters
    ----------
    uuid_type:
        The type of UUID to generate. Currently only ``uuid1`` is supported.
        In the future, ``uuid7`` and ``ulid`` may be supported.

    Returns
    -------
    str
        A UUID.
    """
    import uuid

    funcs = {
        "uuid1": uuid.uuid1,
    }
    if id_type not in funcs:
        raise ValueError(f"UUID type {id_type} not supported.")
    return str(funcs[id_type]())


def get_timestamp_from_uuid(uuid: str) -> float:
    """
    Get the time that a UUID was generated.

    Parameters
    ----------
    uuid
        A UUID.

    Returns
    -------
    float
        The time stamp from the UUID.
    """
    id_type = _get_id_type(uuid)
    funcs = {
        "uuid1": _get_uuid_time_v1,
    }
    return funcs[id_type](uuid)


def _get_id_type(uuid: str) -> str:
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
        version = UUID(uuid).version
        if version not in [
            1,
        ]:
            raise ValueError(f"UUID version {version} not supported.")
        res = {
            1: "uuid1",
        }[version]
    except ValueError:
        raise ValueError(f"ID type for {uuid} not recognized.") from None
    return res


def _get_uuid_time_v1(uuid: str) -> float:
    """
    Get the time that a UUID was generated.

    Parameters
    ----------
    uuid
        A UUID.

    Returns
    -------
    float
        The time stamp from the UUID.
    """
    return (UUID(uuid).time - 0x01B21DD213814000) / 1e7
