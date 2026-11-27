import pytest


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

    # set the path to load settings from
    config_file_path = str(Path.cwd() / "config.yaml")
    os.environ["JOBFLOW_CONFIG_FILE"] = config_file_path

    # A warning should appear if config file is empty
    with open(config_file_path, "w") as f:
        pass

    with pytest.warns(UserWarning, match="An empty JobFlow config file was located at"):
        settings = JobflowSettings()

    # An error should be thrown if the file exists and
    # contains badly formatted contents
    with open(config_file_path, "w") as f:
        f.write("Some text that sadly is not yaml")

    with pytest.raises(ValueError, match="A JobFlow configuration"):
        settings = JobflowSettings()

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


def _isolate_jobflow_environment(monkeypatch, config_file):
    import os

    for name in tuple(os.environ):
        if name.startswith("JOBFLOW_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("JOBFLOW_CONFIG_FILE", str(config_file))


def _memory_store_spec(collection_name):
    return {
        "docs_store": {
            "type": "MemoryStore",
            "collection_name": collection_name,
        }
    }


def test_settings_job_store_filename_from_environment_yaml(tmp_path, monkeypatch):
    from maggma.stores import MemoryStore
    from monty.serialization import dumpfn

    from jobflow import JobStore
    from jobflow.settings import JobflowSettings

    config_file = tmp_path / "settings.yaml"
    environment_store_file = tmp_path / "environment-store.yaml"
    dumpfn({"JOB_STORE": _memory_store_spec("filename_config_default")}, config_file)
    dumpfn(_memory_store_spec("filename_environment_yaml"), environment_store_file)
    _isolate_jobflow_environment(monkeypatch, config_file)

    config_settings = JobflowSettings()
    assert (
        config_settings.JOB_STORE.docs_store.collection_name
        == "filename_config_default"
    )

    monkeypatch.setenv("JOBFLOW_JOB_STORE", str(environment_store_file))
    environment_settings = JobflowSettings()
    assert isinstance(environment_settings.JOB_STORE, JobStore)
    assert isinstance(environment_settings.JOB_STORE.docs_store, MemoryStore)
    assert (
        environment_settings.JOB_STORE.docs_store.collection_name
        == "filename_environment_yaml"
    )


def test_settings_job_store_filename_from_environment_serialized_json(
    tmp_path, monkeypatch
):
    from maggma.stores import MemoryStore
    from monty.serialization import dumpfn

    from jobflow import JobStore
    from jobflow.settings import JobflowSettings

    config_file = tmp_path / "settings.yaml"
    store_file = tmp_path / "environment-store.json"
    dumpfn(
        {"JOB_STORE": _memory_store_spec("serialized_json_config_default")},
        config_file,
    )
    dumpfn(
        JobStore(MemoryStore(collection_name="filename_environment_json")), store_file
    )
    _isolate_jobflow_environment(monkeypatch, config_file)
    monkeypatch.setenv("JOBFLOW_JOB_STORE", str(store_file))

    settings = JobflowSettings()
    assert isinstance(settings.JOB_STORE, JobStore)
    assert settings.JOB_STORE.docs_store.collection_name == "filename_environment_json"


def test_settings_job_store_filename_from_initialization_precedence(
    tmp_path, monkeypatch
):
    from maggma.stores import MemoryStore
    from monty.serialization import dumpfn

    from jobflow import JobStore
    from jobflow.settings import JobflowSettings

    config_file = tmp_path / "settings.yaml"
    environment_store_file = tmp_path / "environment-store.json"
    initialization_store_file = tmp_path / "initialization-store.yaml"
    dumpfn({"JOB_STORE": _memory_store_spec("precedence_config_default")}, config_file)
    dumpfn(
        JobStore(MemoryStore(collection_name="precedence_environment")),
        environment_store_file,
    )
    dumpfn(
        _memory_store_spec("precedence_initialization"),
        initialization_store_file,
    )
    _isolate_jobflow_environment(monkeypatch, config_file)
    monkeypatch.setenv("JOBFLOW_JOB_STORE", str(environment_store_file))

    settings = JobflowSettings(JOB_STORE=str(initialization_store_file))
    assert isinstance(settings.JOB_STORE, JobStore)
    assert settings.JOB_STORE.docs_store.collection_name == "precedence_initialization"


def test_settings_job_store_filename_errors_do_not_use_defaults(tmp_path, monkeypatch):
    from json import JSONDecodeError

    from monty.serialization import dumpfn
    from pydantic import ValidationError

    from jobflow.settings import JobflowSettings

    config_file = tmp_path / "settings.yaml"
    dumpfn({"JOB_STORE": _memory_store_spec("must_not_be_selected")}, config_file)
    _isolate_jobflow_environment(monkeypatch, config_file)

    missing_file = tmp_path / "missing-store.json"
    monkeypatch.setenv("JOBFLOW_JOB_STORE", str(missing_file))
    with pytest.raises((FileNotFoundError, ValidationError)):
        JobflowSettings()

    malformed_file = tmp_path / "malformed-store.json"
    malformed_file.write_text("{ invalid JSON", encoding="utf-8")
    monkeypatch.setenv("JOBFLOW_JOB_STORE", str(malformed_file))
    with pytest.raises((JSONDecodeError, ValidationError)):
        JobflowSettings()


def test_settings_job_store_filename_from_environment_subprocess(tmp_path):
    import hashlib
    import importlib.util
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    from monty.serialization import dumpfn

    config_file = tmp_path / "missing-settings.yaml"
    store_file = tmp_path / "subprocess-store.yaml"
    dumpfn(_memory_store_spec("subprocess_environment_filename"), store_file)
    parent_spec = importlib.util.find_spec("jobflow")
    assert parent_spec is not None
    assert parent_spec.submodule_search_locations is not None
    parent_package = Path(next(iter(parent_spec.submodule_search_locations))).resolve()
    parent_settings = parent_package / "settings.py"
    parent_identity = {
        "package": str(parent_package),
        "settings_sha256": hashlib.sha256(parent_settings.read_bytes()).hexdigest(),
    }
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("JOBFLOW_")
    }
    environment["JOBFLOW_CONFIG_FILE"] = str(config_file)
    environment["JOBFLOW_JOB_STORE"] = str(store_file)
    script = (
        "import hashlib\n"
        "import importlib.util\n"
        "import json\n"
        "from pathlib import Path\n"
        "spec = importlib.util.find_spec('jobflow')\n"
        "assert spec is not None and spec.submodule_search_locations is not None\n"
        "package = Path(next(iter(spec.submodule_search_locations))).resolve()\n"
        "settings = package / 'settings.py'\n"
        "identity = {'package': str(package), 'settings_sha256': "
        "hashlib.sha256(settings.read_bytes()).hexdigest()}\n"
        "print(json.dumps(identity), flush=True)\n"
        "from jobflow import SETTINGS\n"
        "print('LOADED_COLLECTION=' + SETTINGS.JOB_STORE.docs_store.collection_name)\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        cwd=tmp_path,
        env=environment,
        text=True,
        check=False,
    )

    output_lines = result.stdout.splitlines()
    assert output_lines, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    try:
        child_identity = json.loads(output_lines[0])
    except json.JSONDecodeError:
        pytest.fail(f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}")
    assert child_identity == parent_identity, (
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "LOADED_COLLECTION=subprocess_environment_filename" in output_lines, (
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_settings_job_store_preserves_objects_and_serialized_inputs(
    monkeypatch, tmp_path
):
    from maggma.stores import MemoryStore

    from jobflow import JobStore
    from jobflow.settings import JobflowSettings

    _isolate_jobflow_environment(monkeypatch, tmp_path / "missing-settings.yaml")

    store = JobStore(MemoryStore(collection_name="identity_store"))
    object_settings = JobflowSettings(JOB_STORE=store)
    assert object_settings.JOB_STORE is store

    docs_store = MemoryStore(collection_name="constructor_docs_store")
    data_store = MemoryStore(collection_name="constructor_data_store")
    constructor_settings = JobflowSettings(
        JOB_STORE={
            "docs_store": docs_store,
            "additional_stores": {"data": data_store},
        }
    )
    assert constructor_settings.JOB_STORE.docs_store is docs_store
    assert constructor_settings.JOB_STORE.additional_stores["data"] is data_store

    serialized_settings = JobflowSettings(JOB_STORE=store.as_dict())
    assert isinstance(serialized_settings.JOB_STORE, JobStore)
    assert serialized_settings.JOB_STORE.docs_store.collection_name == "identity_store"
