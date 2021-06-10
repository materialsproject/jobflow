import pytest


@pytest.fixture(scope="session")
def test_data():
    from pathlib import Path

    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / "test_data"
    return test_dir.resolve()


@pytest.fixture(scope="session")
def database():
    return "jobflow_test"


@pytest.fixture(scope="session")
def mongo_jobstore(database):
    from maggma.stores import MongoStore

    from jobflow import JobStore

    store = JobStore(MongoStore(database, "outputs"))
    store.connect()
    return store


@pytest.fixture(scope="function")
def memory_jobstore():
    from maggma.stores import MemoryStore

    from jobflow import JobStore

    store = JobStore(MemoryStore())
    store.connect()

    return store


@pytest.fixture(scope="function")
def memory_data_jobstore():
    from maggma.stores import MemoryStore

    from jobflow import JobStore

    store = JobStore(MemoryStore(), additional_stores={"data": MemoryStore()})
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
    return False


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


@pytest.fixture
def no_pydot(monkeypatch):
    import builtins

    import_orig = builtins.__import__

    def mocked_import(name, *args, **kwargs):
        if name == "pydot":
            raise ImportError()
        return import_orig(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mocked_import)


@pytest.fixture
def no_matplotlib(monkeypatch):
    import builtins

    import_orig = builtins.__import__

    def mocked_import(name, *args, **kwargs):
        if name == "matplotlib":
            raise ImportError()
        return import_orig(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mocked_import)
