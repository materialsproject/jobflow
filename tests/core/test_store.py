from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from jobflow.core.store import JobStore


@pytest.fixture
def memory_store():
    from maggma.stores import MemoryStore

    store = MemoryStore()
    store.connect()
    return store


def test_basic(memory_store):
    from jobflow import JobStore

    store = JobStore(memory_store)
    store.connect()
    assert store
    assert store.name == "JobStore-mem://memory_db"
    assert store._collection is not None

    store.close()

    store = JobStore(memory_store, load=None)
    store.connect()
    assert store


def test_additional(memory_store):
    pytest.importorskip("moto")

    from copy import deepcopy

    import boto3
    from maggma.stores import MemoryStore, S3Store
    from moto import mock_aws

    from jobflow import JobStore

    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="bucket1")
        index = MemoryStore("index", key="blob_uuid")
        s3_store = S3Store(index, "bucket1", key="blob_uuid")
        store = JobStore(
            memory_store,
            additional_stores={"data": deepcopy(memory_store), "data_s3": s3_store},
        )

        with store as s:
            assert s
            assert s.name == "JobStore-mem://memory_db"
            assert s._collection is not None
            assert s.additional_stores["data_s3"].searchable_fields == [
                "job_uuid",
                "job_index",
            ]


def _store_config_state(store):
    return (
        store.key,
        list(store.searchable_fields) if hasattr(store, "searchable_fields") else None,
        tuple(store.paths) if hasattr(store, "paths") else None,
        getattr(store, "read_only", None),
    )


def _file_state(path):
    import os

    return (
        path.is_symlink(),
        os.readlink(path) if path.is_symlink() else None,
        path.read_bytes() if path.is_file() else None,
    )


def test_jobstore_rejects_same_store_before_mutating_any_entries():
    from maggma.stores import GridFSStore

    from jobflow import JobStore

    docs_store = GridFSStore(
        database="jobflow_test",
        collection_name="docs",
        key="docs_key",
        searchable_fields=["docs_field"],
    )
    innocent_store = GridFSStore(
        database="jobflow_test",
        collection_name="innocent",
        key="innocent_key",
        searchable_fields=["innocent_field"],
    )
    stores = [docs_store, innocent_store]
    state_before = [_store_config_state(store) for store in stores]

    with pytest.raises(ValueError, match="bad"):
        JobStore(docs_store, {"innocent": innocent_store, "bad": docs_store})

    assert [_store_config_state(store) for store in stores] == state_before


@pytest.mark.parametrize(
    "alias_kind", ["same", "relative_absolute", "symlink", "hardlink"]
)
def test_jobstore_rejects_shared_writable_json_paths_without_mutation(
    tmp_path, monkeypatch, alias_kind
):
    import os
    from pathlib import Path

    from maggma.stores import JSONStore

    from jobflow import JobStore

    shared_path = tmp_path / "shared.json"
    shared_path.write_text("[]", encoding="utf-8")
    innocent_path = tmp_path / "innocent.json"

    if alias_kind == "same":
        docs_path = shared_path
        additional_path = shared_path
    elif alias_kind == "relative_absolute":
        monkeypatch.chdir(tmp_path)
        docs_path = Path("shared.json")
        additional_path = shared_path
    elif alias_kind == "symlink":
        additional_path = tmp_path / "symlink.json"
        additional_path.symlink_to(shared_path)
        docs_path = shared_path
    else:
        additional_path = tmp_path / "hardlink.json"
        try:
            os.link(shared_path, additional_path)
        except OSError as exc:
            pytest.skip(f"hard links are unavailable: {exc}")
        docs_path = shared_path

    docs_store = JSONStore(str(docs_path), read_only=False, key="docs_key")
    innocent_store = JSONStore(str(innocent_path), read_only=True, key="innocent_key")
    bad_store = JSONStore(str(additional_path), read_only=False, key="bad_key")
    stores = [docs_store, innocent_store, bad_store]
    state_before = [_store_config_state(store) for store in stores]
    paths = [shared_path, innocent_path]
    if additional_path != shared_path:
        paths.append(additional_path)
    files_before = {path: _file_state(path) for path in paths}

    with pytest.raises(ValueError, match="bad"):
        JobStore(docs_store, {"innocent": innocent_store, "bad": bad_store})

    assert [_store_config_state(store) for store in stores] == state_before
    assert {path: _file_state(path) for path in paths} == files_before


@pytest.mark.parametrize(
    ("docs_read_only", "additional_read_only", "conflicts"),
    [
        (False, False, True),
        (False, True, True),
        (True, False, True),
        (True, True, False),
    ],
)
def test_jobstore_checks_json_path_conflicts_only_when_a_store_writes(
    tmp_path, docs_read_only, additional_read_only, conflicts
):
    from maggma.stores import JSONStore

    from jobflow import JobStore

    path = tmp_path / "shared.json"
    path.write_text("[]", encoding="utf-8")
    docs_store = JSONStore(str(path), read_only=docs_read_only, key="docs_key")
    additional_store = JSONStore(
        str(path), read_only=additional_read_only, key="additional_key"
    )
    state_before = [
        _store_config_state(store) for store in [docs_store, additional_store]
    ]
    file_before = _file_state(path)

    if conflicts:
        with pytest.raises(ValueError, match="data"):
            JobStore(docs_store, {"data": additional_store})
        assert [
            _store_config_state(store) for store in [docs_store, additional_store]
        ] == state_before
    else:
        JobStore(docs_store, {"data": additional_store})

    assert _file_state(path) == file_before


def test_jobstore_roundtrips_with_distinct_json_paths(tmp_path):
    from maggma.stores import JSONStore

    from jobflow import JobStore

    docs_path = tmp_path / "docs.json"
    blobs_path = tmp_path / "blobs.json"
    with JobStore(
        JSONStore(str(docs_path), read_only=False),
        {"data": JSONStore(str(blobs_path), read_only=False)},
        save={"data": "payload"},
    ) as store:
        store.update({"uuid": "job-1", "index": 0, "payload": {"value": "ok"}})

    with JobStore(
        JSONStore(str(docs_path), read_only=False),
        {"data": JSONStore(str(blobs_path), read_only=False)},
        save={"data": "payload"},
    ) as store:
        result = store.query_one({"uuid": "job-1"}, load={"data": True})

    assert result["payload"] == {"value": "ok"}


def test_jobstore_accepts_independent_read_only_json_paths(tmp_path):
    from maggma.stores import JSONStore

    from jobflow import JobStore

    docs_path = tmp_path / "docs.json"
    blobs_path = tmp_path / "blobs.json"
    docs_path.write_text("[]", encoding="utf-8")
    blobs_path.write_text("[]", encoding="utf-8")

    JobStore(
        JSONStore(str(docs_path), read_only=True),
        {"data": JSONStore(str(blobs_path), read_only=True)},
    )

    assert docs_path.read_text(encoding="utf-8") == "[]"
    assert blobs_path.read_text(encoding="utf-8") == "[]"


def test_jobstore_distinguishes_literal_tilde_path_from_home_path(
    tmp_path, monkeypatch
):
    from pathlib import Path
    from uuid import uuid4

    from maggma.stores import JSONStore

    from jobflow import JobStore

    monkeypatch.chdir(tmp_path)
    (tmp_path / "~").mkdir()
    unique_name = f"jobflow-{uuid4()}.json"

    JobStore(
        JSONStore(f"~/{unique_name}", read_only=False),
        {"data": JSONStore(str(Path.home() / unique_name), read_only=False)},
    )


def test_jobstore_roundtrips_with_literal_tilde_path(tmp_path, monkeypatch):
    from maggma.stores import JSONStore

    from jobflow import JobStore

    monkeypatch.chdir(tmp_path)
    (tmp_path / "~").mkdir()
    docs_path = "~/docs.json"
    blobs_path = "blobs.json"

    with JobStore(
        JSONStore(docs_path, read_only=False),
        {"data": JSONStore(blobs_path, read_only=False)},
        save={"data": "payload"},
    ) as store:
        store.update({"uuid": "job-1", "index": 0, "payload": {"value": "ok"}})

    with JobStore(
        JSONStore(docs_path, read_only=False),
        {"data": JSONStore(blobs_path, read_only=False)},
        save={"data": "payload"},
    ) as store:
        result = store.query_one({"uuid": "job-1"}, load={"data": True})

    assert result["payload"] == {"value": "ok"}


def test_jobstore_roundtrips_with_independent_default_memory_stores():
    from maggma.stores import MemoryStore

    from jobflow import JobStore

    with JobStore(
        MemoryStore(), {"data": MemoryStore()}, save={"data": "payload"}
    ) as store:
        store.update({"uuid": "job-1", "index": 0, "payload": {"value": "ok"}})
        result = store.query_one({"uuid": "job-1"}, load={"data": True})

    assert result["payload"] == {"value": "ok"}


def test_doc_update_query(memory_jobstore):
    # all standard mongo updates should work fine
    d = {"index": 1, "uuid": 1, "e": 6, "d": 4}
    memory_jobstore.update(d)
    results = memory_jobstore.query_one(criteria={"d": {"$exists": 1}})
    assert results["d"] == 4

    d = {"index": 1, "uuid": 1, "e": 6, "d": 4}
    memory_jobstore.update(d, save=False)
    results = memory_jobstore.query_one(criteria={"d": {"$exists": 1}})
    assert results["d"] == 4

    d = [{"index": 1, "uuid": 2, "e": 7, "d": 8, "f": 9}]
    memory_jobstore.update(d, key=["d", "f"])
    result = memory_jobstore.query_one(criteria={"d": 8, "f": 9}, properties=["e"])
    assert result["e"] == 7

    result = list(memory_jobstore.query(criteria={"d": 8, "f": 9}, properties=["e"]))
    assert len(result) == 1

    result = list(memory_jobstore.query(criteria={"d": 8, "f": 9}, properties={"e": 1}))
    assert len(result) == 1


def test_data_update(memory_data_jobstore):
    from monty.json import MSONable

    d = {"index": 1, "uuid": 1, "e": 6, "d": 4, "data": [1, 2, 3, 4]}
    memory_data_jobstore.update(d, save={"data": "data"})

    c = {"d": {"$exists": 1}}
    results = memory_data_jobstore.query_one(c, load={"data": "data"})
    assert results["data"] == [1, 2, 3, 4]

    results = memory_data_jobstore.query_one(c, load={"data": ["data"]})
    assert results["data"] == [1, 2, 3, 4]

    results = memory_data_jobstore.query_one(c, load={"data": True})
    assert results["data"] == [1, 2, 3, 4]

    results = memory_data_jobstore.query_one(c, load=True)
    assert results["data"] == [1, 2, 3, 4]

    results = memory_data_jobstore.query_one(c, load={})
    assert isinstance(results["data"], dict)
    assert "@class" in results["data"]
    assert "@module" in results["data"]
    assert "blob_uuid" in results["data"]

    results = memory_data_jobstore.query_one(c, load={"data": False})
    assert isinstance(results["data"], dict)
    assert "@class" in results["data"]
    assert "@module" in results["data"]
    assert "blob_uuid" in results["data"]

    results = memory_data_jobstore.query_one(c, load=False)
    assert isinstance(results["data"], dict)
    assert "@class" in results["data"]
    assert "@module" in results["data"]
    assert "blob_uuid" in results["data"]

    results = memory_data_jobstore.query_one(c, load=None)
    assert isinstance(results["data"], dict)
    assert "@class" in results["data"]
    assert "@module" in results["data"]
    assert "blob_uuid" in results["data"]

    # test bad store name fails
    results["data"]["store"] = "bad_store"
    memory_data_jobstore.update(results)
    with pytest.raises(ValueError, match="Unrecognised additional store name"):
        memory_data_jobstore.query_one(c, load=True)

    with pytest.raises(
        ValueError, match="Unrecognised additional store name: bad_store"
    ):
        memory_data_jobstore.update(d, save={"bad_store": "data"})

    d = {"index": 2, "uuid": 2, "e": 6, "x": 4, "data2": [1, 2, 3]}
    memory_data_jobstore.update(d, save={"data": ["data2", "data1", "data"]})

    c = {"x": {"$exists": 1}}
    results = memory_data_jobstore.query_one(c, load={"data": "data2"})
    assert results["data2"] == [1, 2, 3]

    # test msonable store and load

    global MyClass

    class MyClass(MSONable):
        def __init__(self, a):
            self.a = a

    d = {"index": 1, "uuid": 3, "x": 10, "output": {"a": 1, "b": MyClass(5)}}
    memory_data_jobstore.update(d, save={"data": ["x", MyClass]})

    c = {"uuid": 3}
    results = memory_data_jobstore.query_one(c, load=False)
    assert isinstance(results["output"]["b"], dict)
    assert "blob_uuid" in results["output"]["b"]
    assert isinstance(results["x"], dict)
    assert "blob_uuid" in results["x"]

    results = memory_data_jobstore.query_one(c, load=True)
    assert isinstance(results["output"]["b"], dict)
    assert results["output"]["b"]["@class"] == "MyClass"
    assert results["output"]["b"]["a"] == 5
    assert results["x"] == 10

    results = memory_data_jobstore.query_one(c, load={"data": MyClass})
    assert isinstance(results["output"]["b"], dict)
    assert results["output"]["b"]["@class"] == "MyClass"
    assert results["output"]["b"]["a"] == 5
    assert isinstance(results["x"], dict)
    assert "blob_uuid" in results["x"]

    results = memory_data_jobstore.query_one(c, load={"data": [MyClass, "x"]})
    assert isinstance(results["output"]["b"], dict)
    assert results["output"]["b"]["@class"] == "MyClass"
    assert results["output"]["b"]["a"] == 5
    assert results["x"] == 10

    # test enum
    from jobflow.utils import ValueEnum

    class MyEnum(ValueEnum):
        A = "A"
        B = "B"

    d = {"index": 1, "uuid": 3, "x": 10, "output": {"a": 1, MyEnum.B: 101}}
    memory_data_jobstore.update(d, save={"data": ["x", MyEnum.B]})

    c = {"uuid": 3}
    results = memory_data_jobstore.query_one(c, load=False)
    assert isinstance(results["output"]["B"], dict)
    assert "blob_uuid" in results["output"]["B"]
    assert isinstance(results["x"], dict)
    assert "blob_uuid" in results["x"]

    results = memory_data_jobstore.query_one(c, load=True)
    assert results["output"]["B"] == 101
    assert results["x"] == 10

    results = memory_data_jobstore.query_one(c, load={"data": MyEnum.B})
    assert results["output"]["B"] == 101
    assert isinstance(results["x"], dict)
    assert "blob_uuid" in results["x"]

    results = memory_data_jobstore.query_one(c, load={"data": [MyEnum.B, "x"]})
    assert results["output"]["B"] == 101
    assert results["x"] == 10

    # test None
    data2 = [2, 2]
    data3 = [3, 3]
    d = {"index": 1, "uuid": 4, "data1": None, "data2": data2, "data3": data3}
    memory_data_jobstore.update(d, save={"data": ["data1", "data2", "data3"]})

    c = {"uuid": 4}
    results = memory_data_jobstore.query_one(c, load=None)
    assert results["data1"] is None

    results = memory_data_jobstore.query_one(c, load=False)
    assert results["data1"] is None

    c = {"job_uuid": 4}
    data_store = memory_data_jobstore.additional_stores["data"]
    data_fields = [doc["data"] for doc in data_store.query(c)]
    assert data2 in data_fields
    assert data3 in data_fields
    assert None not in data_fields


def test_nested_msonable(memory_data_jobstore):
    from monty.json import MSONable

    class Child(MSONable):
        def __init__(self, x):
            self.x = x

    class Parent(MSONable):
        def __init__(self, child):
            self.child = child

    parent = Parent(child=Child(x=5))
    d = {"index": 1, "uuid": 1, "parent": parent}
    memory_data_jobstore: JobStore
    memory_data_jobstore.update(d, save={"data": [Parent, Child]})
    ds = memory_data_jobstore.additional_stores["data"]
    classes = ds.distinct("@class")
    assert set(classes) == {
        "Parent",
    }


def test_count(memory_jobstore):
    d = {"index": 1, "uuid": 1, "a": 1, "b": 2, "c": 3, "data": [1, 2, 3, 4]}
    memory_jobstore.update(d)
    assert memory_jobstore.count() == 1

    d = {"index": 1, "uuid": 2, "aa": 1, "b": 2, "c": 3, "data": [1, 2]}
    memory_jobstore.update(d)
    assert memory_jobstore.count() == 2
    assert memory_jobstore.count({"a": 1}) == 1


def test_distinct(memory_jobstore):
    d1 = {"a": 1, "b": 2, "c": 3, "uuid": 1, "index": 1}
    d2 = {"a": 4, "d": 5, "e": 6, "g": {"h": 1}, "uuid": 2, "index": 1}
    memory_jobstore.update(d1)
    memory_jobstore.update(d2)
    assert set(memory_jobstore.distinct("a")) == {1, 4}

    # Test list distinct functionality
    d1 = {"a": 4, "d": 6, "e": 7, "index": 1, "uuid": 3}
    d2 = {"a": 4, "d": 6, "g": {"h": 2}, "index": 1, "uuid": 4}
    memory_jobstore.update(d1)
    memory_jobstore.update(d2)

    # Test distinct subdocument functionality
    ghs = memory_jobstore.distinct("g.h")
    assert set(ghs) == {1, 2}

    # Test when key doesn't exist
    assert memory_jobstore.distinct("blue") == []

    # Test when null is a value
    d1 = {"i": None, "index": 1, "uuid": 3}
    memory_jobstore.update(d1)
    assert memory_jobstore.distinct("i") == [None]


def test_groupby(memory_jobstore):
    memory_jobstore.update(
        [
            {"e": 7, "d": 9, "f": 9, "uuid": 1, "index": 1},
            {"e": 7, "d": 9, "f": 10, "uuid": 2, "index": 1},
            {"e": 8, "d": 9, "f": 11, "uuid": 3, "index": 1},
            {"e": 9, "d": 10, "f": 12, "uuid": 4, "index": 1},
        ],
        key="f",
    )
    data = list(memory_jobstore.groupby("d"))
    assert len(data) == 2
    grouped_by_9 = next(g[1] for g in data if g[0]["d"] == 9)
    assert len(grouped_by_9) == 3
    grouped_by_10 = next(g[1] for g in data if g[0]["d"] == 10)
    assert len(grouped_by_10) == 1

    data = list(memory_jobstore.groupby(["e", "d"]))
    assert len(data) == 3

    data = list(memory_jobstore.groupby(["e", "d"], properties={"uuid": 1}))
    assert len(data) == 3

    data = list(memory_jobstore.groupby(["e", "d"], properties=["uuid"]))
    assert len(data) == 3


def test_remove_docs(memory_jobstore, memory_data_jobstore):
    d1 = {"a": 1, "b": 2, "c": 3, "index": 1, "uuid": 1}
    d2 = {"a": 4, "d": 5, "e": 6, "g": {"h": 1}, "index": 1, "uuid": 2}
    memory_jobstore.update(d1)
    memory_jobstore.update(d2)
    memory_jobstore.remove_docs({"a": 1})
    assert len(list(memory_jobstore.query({"a": 1}))) == 0
    assert len(list(memory_jobstore.query({"a": 4}))) == 1

    d1 = {"a": 1, "b": 2, "c": 3, "index": 1, "uuid": 1}
    d2 = {"a": 4, "d": 5, "c": 3, "g": {"h": 1}, "index": 1, "uuid": 2}
    memory_data_jobstore.update(d1, save={"data": "c"})
    memory_data_jobstore.update(d2, save={"data": "c"})
    memory_data_jobstore.remove_docs({"a": 1})

    data_store = memory_data_jobstore.additional_stores["data"]
    assert len(list(memory_data_jobstore.query({"a": 1}))) == 0
    assert len(list(memory_data_jobstore.query({"a": 4}))) == 1

    assert len(list(data_store.query({"job_uuid": 1}))) == 0
    assert len(list(data_store.query({"job_uuid": 2}))) == 1


def test_get_output(memory_jobstore):
    from jobflow import OnMissing

    docs = [
        {"uuid": "1", "index": 1, "output": "xyz"},
        {"uuid": "1", "index": 2, "output": "abc"},
        {"uuid": "1", "index": 3, "output": 123},
        {"uuid": "1", "index": 4, "output": "a"},
        {"uuid": "2", "index": 1, "output": "12345"},
        {"uuid": "3", "index": 1, "output": "test"},
    ]
    memory_jobstore.update(docs)

    output = memory_jobstore.get_output("1")
    assert output == "a"

    output = memory_jobstore.get_output("1", which="last")
    assert output == "a"

    output = memory_jobstore.get_output("1", which="first")
    assert output == "xyz"

    output = memory_jobstore.get_output("1", which="all")
    assert output == ["xyz", "abc", 123, "a"]

    output = memory_jobstore.get_output("1", which=1)
    assert output == "xyz"

    output = memory_jobstore.get_output("1", which=3)
    assert output == 123

    with pytest.raises(ValueError, match="UUID: 1 has no outputs"):
        memory_jobstore.get_output(1, which="first")

    with pytest.raises(ValueError, match="UUID: 1 has no outputs"):
        memory_jobstore.get_output(1, which="all")

    # test resolving reference in output of job
    r = {"@module": "jobflow.core.reference", "@class": "OutputReference", "uuid": "10"}
    docs = [
        {"uuid": "5", "index": 1, "output": r},
        {"uuid": "6", "index": 1, "output": {"a": r, "b": 5}},
        {"uuid": "10", "index": 1, "output": "xyz"},
    ]
    memory_jobstore.update(docs)
    assert memory_jobstore.get_output("5") == "xyz"
    assert memory_jobstore.get_output("6") == {"a": "xyz", "b": 5}

    # test missing reference in output of job
    r = {"@module": "jobflow.core.reference", "@class": "OutputReference", "uuid": "a"}
    memory_jobstore.update({"uuid": "8", "index": 1, "output": r})
    with pytest.raises(
        ValueError, match="Could not resolve reference - a not in store or"
    ):
        memory_jobstore.get_output("8", on_missing=OnMissing.ERROR)

    assert memory_jobstore.get_output("8", on_missing=OnMissing.NONE) is None
    assert memory_jobstore.get_output("8", on_missing=OnMissing.PASS).uuid == r["uuid"]

    # test catching circular reference
    r = {"@module": "jobflow.core.reference", "@class": "OutputReference", "uuid": "5"}
    memory_jobstore.update({"uuid": "5", "index": 1, "output": r})
    with pytest.raises(RuntimeError):
        memory_jobstore.get_output("5")

    r1 = {"@module": "jobflow.core.reference", "@class": "OutputReference", "uuid": "5"}
    memory_jobstore.update({"uuid": "5", "index": 1, "output": r1})
    memory_jobstore.update({"uuid": "5", "index": 2, "output": r1})
    with pytest.raises(RuntimeError):
        memory_jobstore.get_output("5", which="all")


def test_from_db_file(test_data):
    from jobflow import JobStore

    ms = JobStore.from_file(test_data / "db.yaml")
    ms.connect()
    assert ms.docs_store.name == "mongo://localhost/jobflow_unittest/outputs"
    assert ms.additional_stores == {}

    # test gridfs
    ms = JobStore.from_file(test_data / "db_gridfs.yaml")
    ms.connect()
    data_store = ms.additional_stores["data"]
    assert ms.docs_store.name == "mongo://localhost/jobflow_unittest/outputs"
    assert data_store.name == "gridfs://localhost/jobflow_unittest/outputs_blobs"

    # test serialized
    ms = JobStore.from_file(test_data / "db_serialized.json")
    ms.connect()
    data_store = ms.additional_stores["data"]
    assert ms.docs_store.name == "mongo://localhost/jobflow_unittest/outputs"
    assert data_store.name == "gridfs://localhost/jobflow_unittest/outputs_blobs"

    # test bad file
    with pytest.raises(ValueError, match="Unrecognised database file format"):
        JobStore.from_file(test_data / "db_bad.yaml")


def test_from_dict_spec():
    from jobflow import JobStore

    dict_spec = {
        "docs_store": {
            "type": "MongoStore",
            "database": "jobflow_unittest",
            "collection_name": "outputs",
            "host": "localhost",
            "port": 27017,
        }
    }
    JobStore.from_dict_spec(dict_spec)
    assert "type" in dict_spec["docs_store"]
    dict_spec = {
        "docs_store": {
            "type": "MongoStore",
            "database": "jobflow_unittest",
            "collection_name": "outputs",
            "host": "localhost",
            "port": 27017,
        },
        "additional_stores": {
            "data": {
                "type": "GridFSStore",
                "database": "jobflow_unittest",
                "collection_name": "outputs_blobs",
                "host": "localhost",
                "port": 27017,
            }
        },
    }
    JobStore.from_dict_spec(dict_spec)
    assert "type" in dict_spec["docs_store"]
    assert dict_spec["docs_store"]["type"] == "MongoStore"
    assert "type" in dict_spec["additional_stores"]["data"]
    assert dict_spec["additional_stores"]["data"]["type"] == "GridFSStore"


def test_ensure_index(memory_jobstore):
    assert memory_jobstore.ensure_index("test_key")
    # TODO: How to check for exception?
