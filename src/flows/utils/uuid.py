"""Tools for generating UUIDs."""


def suuid() -> str:
    """
    Generate a string UUID (universally unique identifier).

    Uses the UUID4 specification.

    Returns
    -------
    str
        A UUID.
    """
    from uuid import uuid4

    return str(uuid4())
