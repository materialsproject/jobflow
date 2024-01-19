def test_suuid():
    from jobflow.utils import suid

    uuid = suid()
    assert isinstance(uuid, str)
    assert len(uuid) == 36
