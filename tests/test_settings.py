def test_settings_init():
    import os

    from maggma.stores import MemoryStore

    # set the config file to a not existing path so that it does not
    # pick the local configuration
    os.environ["JOBFLOW_CONFIG_FILE"] = "/some/not/existing/path"

    from jobflow import SETTINGS

    # assert default job store initialised correctly
    assert isinstance(SETTINGS.JOB_STORE.docs_store, MemoryStore)
    assert len(SETTINGS.JOB_STORE.additional_stores) == 0
    data_store = SETTINGS.JOB_STORE.additional_stores["data"]
    assert len(SETTINGS.JOB_STORE.additional_stores) == 1
    assert isinstance(data_store, MemoryStore)
    assert data_store is SETTINGS.JOB_STORE.additional_stores["data"]


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

    s3_store_spec = {
        "docs_store": {
            "type": "MemoryStore",
            "collection_name": "docs_store_123",
        },
        "additional_stores": {
            "data": {
                "type": "S3Store",
                "bucket": "bucket_123",
                "index": {
                    "type": "MemoryStore",
                },
            }
        },
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

    dumpfn({"JOB_STORE": s3_store_spec}, "config.yaml")
    settings = JobflowSettings()
    assert settings.JOB_STORE.additional_stores["data"].bucket == "bucket_123"

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
