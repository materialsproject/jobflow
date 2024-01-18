import pytest


def test_uuid1():
    from uuid import UUID

    from jobflow.utils.uuid import get_timestamp_from_uuid, suuid

    uuid = suuid("uuid1")
    assert UUID(uuid).version == 1
    assert isinstance(get_timestamp_from_uuid(uuid), float)

    with pytest.raises(ValueError, match="UUID type uuid2 not supported."):
        suuid("uuid2")

    with pytest.raises(ValueError, match="ID type for FAKEUUID not recognized."):
        get_timestamp_from_uuid("FAKEUUID")
