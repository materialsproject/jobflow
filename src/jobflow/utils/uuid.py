"""Tools for generating UUIDs."""

from monty.dev import deprecated


@deprecated(
    message="The UUID system will be replace with UID that contains both UUID and ULID."
)
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
