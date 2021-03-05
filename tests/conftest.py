import pytest


@pytest.fixture(scope="session")
def test_database():
    return "activities_test"


@pytest.fixture(scope="session")
def output_store(test_database):
    from maggma.stores import MongoStore

    return MongoStore(test_database, "activity_outputs")


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
