import pytest


@pytest.fixture(scope="session")
def database():
    return "jobflow_test"


@pytest.fixture(scope="session")
def mongo_jobstore(database):
    from maggma.stores import MongoStore

    from jobflow import JobStore

    store = JobStore.from_store(MongoStore(database, "outputs"))
    store.connect()
    return store


@pytest.fixture(scope="session")
def memory_jobstore():
    from maggma.stores import MemoryStore

    from jobflow import JobStore

    store = JobStore.from_store(MemoryStore())
    store.connect()

    return store


@pytest.fixture
def clean_dir():
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)

    yield

    os.chdir(old_cwd)
    shutil.rmtree(newpath)


@pytest.fixture(scope="session")
def debug_mode():
    return True


@pytest.fixture(scope="session")
def lpad(database, debug_mode):
    from fireworks import LaunchPad

    lpad = LaunchPad(name=database)
    lpad.reset("", require_password=False)
    yield lpad

    if not debug_mode:
        lpad.reset("", require_password=False)
        for coll in lpad.db.list_collection_names():
            lpad.db[coll].drop()
