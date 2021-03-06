import typing

import pytest

typing.TYPE_CHECKING = True  # set type checking to ensure all type hints are valid


@pytest.fixture(scope="session")
def test_database():
    return "activities_test"


@pytest.fixture(scope="session")
def mongo_output_store(test_database):
    from maggma.stores import MongoStore

    store = MongoStore(test_database, "activity_outputs")
    store.connect()
    return store


@pytest.fixture(scope="session")
def output_store():
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
