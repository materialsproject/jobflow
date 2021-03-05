import pytest


@pytest.fixture(scope="session")
def debug_mode():
    return True


@pytest.fixture(scope="session")
def lpad(test_database, debug_mode):
    from fireworks import LaunchPad

    lpad = LaunchPad(name=test_database)
    lpad.reset("", require_password=False)
    yield lpad

    if not debug_mode:
        lpad.reset("", require_password=False)
        for coll in lpad.db.list_collection_names():
            lpad.db[coll].drop()
