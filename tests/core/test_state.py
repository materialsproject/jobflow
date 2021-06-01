def test_state():
    from jobflow import CURRENT_JOB

    # test state is not set when job is not running
    assert CURRENT_JOB.job is None
    assert CURRENT_JOB.store is None
