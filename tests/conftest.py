import pytest


@pytest.fixture(scope="session")
def database():
    return "activities_test"


@pytest.fixture(scope="session")
def mongo_store(database):
    from maggma.stores import MongoStore

    store = MongoStore(database, "activity_outputs")
    store.connect()
    return store


@pytest.fixture(scope="session")
def memory_store():
    from maggma.stores import MemoryStore

    store = MemoryStore()
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
