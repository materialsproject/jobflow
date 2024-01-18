def test_uuid1():
    from uuid import UUID

    from jobflow.utils.uuid import get_timestamp_from_uuid, suuid

    uuid = suuid("uuid1")
    assert UUID(uuid).version == 1

    assert isinstance(get_timestamp_from_uuid(uuid), float)
