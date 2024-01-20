def test_suuid():
    from jobflow.utils import suuid

    uuid = suuid()
    assert isinstance(uuid, str)
    assert len(uuid) == 36
