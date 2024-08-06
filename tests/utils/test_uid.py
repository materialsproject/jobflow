import pytest


def test_uid():
    pytest.importorskip("ulid")

    from uuid import UUID

    from ulid import ULID

    from jobflow.utils.uid import get_timestamp_from_uid, suid

    uuid = suid("uuid1")
    assert UUID(uuid).version == 1
    t1 = get_timestamp_from_uid(uuid)
    assert isinstance(t1, float)

    uuid = suid("uuid4")
    assert UUID(uuid).version == 4

    with pytest.raises(
        ValueError,
        match="UUID4 is randomly generated and not associated with a time stamp.",
    ):
        get_timestamp_from_uid(uuid)

    ulid = suid("ulid")
    assert ULID.from_str(ulid)
    t2 = get_timestamp_from_uid(ulid)
    assert isinstance(t2, float)

    with pytest.raises(ValueError, match="UUID type uuid2 not supported."):
        suid("uuid2")

    with pytest.raises(ValueError, match="ID type for FAKEUUID not recognized."):
        get_timestamp_from_uid("FAKEUUID")

    default_uid = suid()
    assert UUID(default_uid).version == 4
    # assert len(ULID.from_str(default_uid).hex) == 32 # uncomment when ulid is default
