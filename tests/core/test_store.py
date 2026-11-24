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


def _blob_fixture(stores):
    from maggma.stores import MemoryStore

    from jobflow import JobStore

    docs = MemoryStore()
    additional = {name: MemoryStore() for name in stores}
    store = JobStore(docs, additional_stores=additional)
    store.connect()
    return store


@pytest.fixture
def jobflow_source_identity(request, capsys):
    import hashlib
    import inspect
    import json
    from pathlib import Path

    from jobflow import JobStore

    installed_path = Path(inspect.getsourcefile(JobStore)).resolve()
    checkout_path = Path(__file__).resolve().parents[2] / "src/jobflow/core/store.py"
    installed_digest = hashlib.sha256(installed_path.read_bytes()).hexdigest()
    checkout_digest = hashlib.sha256(checkout_path.read_bytes()).hexdigest()
    assert installed_digest == checkout_digest
    metadata = {
        "nodeid": request.node.nodeid,
        "installed_jobstore_source_path": str(installed_path),
        "installed_jobstore_source_sha256": installed_digest,
        "checkout_source_path": str(checkout_path.resolve()),
        "checkout_source_sha256": checkout_digest,
    }
    with capsys.disabled():
        print(f"JOBFLOW_BLOB_RUNTIME {json.dumps(metadata, sort_keys=True)}")


def _assert_blob_record(store, name, placeholder, payload, uuid, index):
    assert placeholder["store"] == name
    assert placeholder["blob_uuid"]
    records = list(
        store.additional_stores[name].query(
            {"blob_uuid": placeholder["blob_uuid"]},
            properties=None,
        )
    )
    assert len(records) == 1
    assert records[0]["blob_uuid"] == placeholder["blob_uuid"]
    assert records[0]["data"] == payload
    assert records[0]["job_uuid"] == uuid
    assert records[0]["job_index"] == index


def _repeat_blob(store, uuid, index, store_name):
    from copy import deepcopy

    payload = {"marker": "saved-payload", "values": [17, 23]}
    sentinel = "unmodified-root-field"
    original = {
        "uuid": uuid,
        "index": index,
        "sentinel": sentinel,
        "output": {"left": payload},
    }
    store.update(
        original,
        save={store_name: [["output", "left"]]},
    )
    original = store.query_one({"uuid": uuid}, load=False, properties=None)
    placeholder = original["output"]["left"]
    assert placeholder["@class"] == ""
    assert placeholder["@module"] == ""
    assert placeholder["blob_uuid"]
    _assert_blob_record(store, placeholder["store"], placeholder, payload, uuid, index)
    repeated = deepcopy(original)
    repeated["output"]["right"] = deepcopy(placeholder)
    store.update(repeated, save=False)
    raw = store.query_one({"uuid": uuid}, load=False, properties=None)
    assert raw["uuid"] == uuid
    assert raw["index"] == index
    assert raw["sentinel"] == sentinel
    assert raw["output"]["left"] == placeholder
    assert raw["output"]["right"] == placeholder
    _assert_blob_record(store, placeholder["store"], placeholder, payload, uuid, index)
    return payload, placeholder, sentinel


def _assert_document_identity(document, uuid, index, sentinel):
    assert document["uuid"] == uuid
    assert document["index"] == index
    assert document["sentinel"] == sentinel


def test_repeated_blob_query_global_true(jobflow_source_identity):
    store = _blob_fixture(["data"])
    uuid, index = "repeated-query", 31
    payload, _, sentinel = _repeat_blob(store, uuid, index, "data")
    results = list(store.query({"uuid": "repeated-query"}, load=True, properties=None))
    _assert_document_identity(results[0], uuid, index, sentinel)
    assert results[0]["output"]["left"] == payload
    assert results[0]["output"]["right"] == payload


def test_repeated_blob_query_one_global_true(jobflow_source_identity):
    store = _blob_fixture(["data"])
    uuid, index = "repeated-query-one", 32
    payload, _, sentinel = _repeat_blob(store, uuid, index, "data")
    result = store.query_one({"uuid": "repeated-query-one"}, load=True, properties=None)
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"]["left"] == payload
    assert result["output"]["right"] == payload


def test_repeated_blob_both_field_names_selected(jobflow_source_identity):
    store = _blob_fixture(["data"])
    uuid, index = "repeated-fields", 33
    payload, _, sentinel = _repeat_blob(store, uuid, index, "data")
    result = store.query_one(
        {"uuid": "repeated-fields"},
        load={"data": ["left", "right"]},
        properties=None,
    )
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"]["left"] == payload
    assert result["output"]["right"] == payload


def test_two_store_true_false_selection_keeps_unselected_placeholder(
    jobflow_source_identity,
):
    store = _blob_fixture(["A", "B"])
    uuid, index = "two-store-boolean", 34
    sentinel = "unmodified-root-field"
    payload_a = {"marker": "A", "values": [1, 2]}
    payload_b = {"marker": "B", "values": [3, 4]}
    store.update(
        {
            "uuid": uuid,
            "index": index,
            "sentinel": sentinel,
            "output": {"A": payload_a},
        },
        save={"A": [["output", "A"]]},
    )
    document = store.query_one({"uuid": uuid}, load=False, properties=None)
    document["output"]["B"] = payload_b
    store.update(document, save={"B": [["output", "B"]]})
    raw = store.query_one({"uuid": uuid}, load=False, properties=None)
    placeholder_a, placeholder_b = raw["output"]["A"], raw["output"]["B"]
    assert placeholder_a["blob_uuid"] != placeholder_b["blob_uuid"]
    _assert_blob_record(store, "A", placeholder_a, payload_a, uuid, index)
    _assert_blob_record(store, "B", placeholder_b, payload_b, uuid, index)
    result = store.query_one(
        {"uuid": uuid}, load={"A": True, "B": False}, properties=None
    )
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"]["A"] == payload_a
    assert result["output"]["B"] == placeholder_b


def test_two_store_same_leaf_selection_is_store_scoped(jobflow_source_identity):
    store = _blob_fixture(["A", "B"])
    uuid, index = "two-store-leaf", 35
    sentinel = "unmodified-root-field"
    payload_a = {"marker": "A-item", "values": [5, 6]}
    payload_b = {"marker": "B-item", "values": [7, 8]}
    store.update(
        {
            "uuid": uuid,
            "index": index,
            "sentinel": sentinel,
            "output": {"left": {"item": payload_a}},
        },
        save={"A": [["output", "left", "item"]]},
    )
    document = store.query_one({"uuid": uuid}, load=False, properties=None)
    document["output"]["right"] = {"item": payload_b}
    store.update(document, save={"B": [["output", "right", "item"]]})
    raw = store.query_one({"uuid": uuid}, load=False, properties=None)
    a, b = raw["output"]["left"]["item"], raw["output"]["right"]["item"]
    _assert_blob_record(store, "A", a, payload_a, uuid, index)
    _assert_blob_record(store, "B", b, payload_b, uuid, index)
    result = store.query_one({"uuid": uuid}, load={"A": "item"}, properties=None)
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"]["left"]["item"] == payload_a
    assert result["output"]["right"]["item"] == b


def test_two_store_same_msonable_class_selection_is_store_scoped(
    jobflow_source_identity,
):
    from monty.json import MSONable

    class Payload(MSONable):
        def __init__(self, value):
            self.value = value

    store = _blob_fixture(["A", "B"])
    uuid, index = "two-store-class", 36
    sentinel = "unmodified-root-field"
    store.update(
        {
            "uuid": uuid,
            "index": index,
            "sentinel": sentinel,
            "output": {"left": Payload("A")},
        },
        save={"A": [["output", "left"]]},
    )
    document = store.query_one({"uuid": uuid}, load=False, properties=None)
    document["output"]["right"] = Payload("B")
    store.update(document, save={"B": [["output", "right"]]})
    raw = store.query_one({"uuid": uuid}, load=False, properties=None)
    a, b = raw["output"]["left"], raw["output"]["right"]
    expected_a = Payload("A").as_dict()
    expected_b = Payload("B").as_dict()
    assert expected_a["value"] == "A"
    assert expected_b["value"] == "B"
    _assert_blob_record(store, "A", a, expected_a, uuid, index)
    _assert_blob_record(store, "B", b, expected_b, uuid, index)
    result = store.query_one({"uuid": uuid}, load={"A": Payload}, properties=None)
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"]["left"] == expected_a
    assert result["output"]["right"] == b


def test_distinct_blob_uuids_same_store_global_true(jobflow_source_identity):
    store = _blob_fixture(["data"])
    uuid, index = "distinct-one-store", 37
    sentinel = "unmodified-root-field"
    first = {"marker": "first", "values": [9]}
    second = {"marker": "second", "values": [10]}
    store.update(
        {
            "uuid": uuid,
            "index": index,
            "sentinel": sentinel,
            "output": {"first": first, "second": second},
        },
        save={"data": ["first", "second"]},
    )
    raw = store.query_one({"uuid": uuid}, load=False, properties=None)
    _assert_blob_record(store, "data", raw["output"]["first"], first, uuid, index)
    _assert_blob_record(store, "data", raw["output"]["second"], second, uuid, index)
    assert raw["output"]["first"]["blob_uuid"] != raw["output"]["second"]["blob_uuid"]
    result = store.query_one({"uuid": uuid}, load=True, properties=None)
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"] == {"first": first, "second": second}


def test_distinct_blob_uuids_across_stores_global_true(jobflow_source_identity):
    store = _blob_fixture(["A", "B"])
    uuid, index = "distinct-two-stores", 38
    sentinel = "unmodified-root-field"
    first, second = {"marker": "A", "values": [11]}, {"marker": "B", "values": [12]}
    store.update(
        {
            "uuid": uuid,
            "index": index,
            "sentinel": sentinel,
            "output": {"first": first},
        },
        save={"A": [["output", "first"]]},
    )
    document = store.query_one({"uuid": uuid}, load=False, properties=None)
    document["output"]["second"] = second
    store.update(document, save={"B": [["output", "second"]]})
    raw = store.query_one({"uuid": uuid}, load=False, properties=None)
    _assert_blob_record(store, "A", raw["output"]["first"], first, uuid, index)
    _assert_blob_record(store, "B", raw["output"]["second"], second, uuid, index)
    assert raw["output"]["first"]["blob_uuid"] != raw["output"]["second"]["blob_uuid"]
    result = store.query_one({"uuid": uuid}, load=True, properties=None)
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"] == {"first": first, "second": second}


def test_repeated_blob_one_selected_occurrence_keeps_sibling_placeholder(
    jobflow_source_identity,
):
    store = _blob_fixture(["data"])
    uuid, index = "one-selected-alias", 39
    payload, placeholder, sentinel = _repeat_blob(store, uuid, index, "data")
    result = store.query_one({"uuid": uuid}, load={"data": "left"}, properties=None)
    _assert_document_identity(result, uuid, index, sentinel)
    assert result["output"]["left"] == payload
    assert result["output"]["right"] == placeholder


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
