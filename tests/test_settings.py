def test_settings_init():
    from maggma.stores import MemoryStore

    from jobflow import SETTINGS

    # assert default job store initialised correctly
    assert isinstance(SETTINGS.JOB_STORE.docs_store, MemoryStore)


def test_settings_object(clean_dir, test_data):
    import os
    from pathlib import Path

    from monty.serialization import dumpfn

    from jobflow import JobStore
    from jobflow.settings import JobflowSettings

    monty_spec = {
        "@module": "jobflow.core.store",
        "@class": "JobStore",
        "@version": "",
        "docs_store": {
            "@module": "maggma.stores.mongolike",
            "@class": "MemoryStore",
            "@version": "0.31.0",
            "collection_name": "memory_db_123",
        },
        "additional_stores": {},
        "save": {},
        "load": False,
    }

    dict_spec = {
        "docs_store": {
            "type": "MongoStore",
            "database": "jobflow_unittest",
            "collection_name": "outputs_567",
            "host": "localhost",
            "port": 27017,
        }
    }

    # set the path to lood settings from
    os.environ["JOBFLOW_CONFIG_FILE"] = str(Path.cwd() / "config.yaml")

    # assert loading monty spec from files works
    dumpfn({"JOB_STORE": monty_spec}, "config.yaml")
    settings = JobflowSettings()
    assert settings.JOB_STORE.docs_store.collection_name == "memory_db_123"

    # assert loading alternative dict spec from files works
    dumpfn({"JOB_STORE": dict_spec}, "config.yaml")
    settings = JobflowSettings()
    assert settings.JOB_STORE.docs_store.collection_name == "outputs_567"

    # assert loading from db file works.
    dumpfn({"JOB_STORE": str(test_data / "db.yaml")}, "config.yaml")
    settings = JobflowSettings()
    assert settings.JOB_STORE.docs_store.collection_name == "outputs"

    # assert loading from serialized file works.
    dumpfn({"JOB_STORE": str(test_data / "db_serialized.json")}, "config.yaml")
    settings = JobflowSettings()
    assert settings.JOB_STORE.docs_store.database == "jobflow_unittest"

    # assert passing a jobflow object works
    settings = JobflowSettings(JOB_STORE=JobStore.from_dict(monty_spec))
    assert settings.JOB_STORE.docs_store.collection_name == "memory_db_123"
