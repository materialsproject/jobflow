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


def _payload_publication_jobstore(additional_store=None, docs_store=None):
    from maggma.stores import MemoryStore

    from jobflow import JobStore

    if docs_store is None:
        docs_store = MemoryStore()
    if additional_store is None:
        additional_store = MemoryStore()
    store = JobStore(docs_store, additional_stores={"data": additional_store})
    store.connect()
    return store, additional_store, docs_store


def _payload_publication_output(marker, values):
    return {"marker": marker, "values": values}


def _seed_payload_publication_index(store, uuid):
    payload = _payload_publication_output("index-one", [3, 5, 8])
    store.update(
        {
            "uuid": uuid,
            "index": 1,
            "sentinel": "preserve-index-one",
            "output": payload,
        },
        save={"data": "output"},
    )
    raw = store.query_one({"uuid": uuid, "index": 1}, load=False, properties=None)
    assert raw["sentinel"] == "preserve-index-one"
    placeholder = raw["output"]
    assert placeholder["store"] == "data"
    assert placeholder["blob_uuid"]
    records = list(
        store.additional_stores["data"].query(
            {"blob_uuid": placeholder["blob_uuid"]}, properties=None
        )
    )
    assert len(records) == 1
    assert records[0]["data"] == payload
    assert records[0]["job_uuid"] == uuid
    assert records[0]["job_index"] == 1
    assert store.get_output(uuid, which=1, load=True) == payload
    return payload


def _make_controlled_payload_store():
    from threading import Event

    from maggma.stores import MemoryStore

    class ControlledMemoryStore(MemoryStore):
        def __init__(self):
            super().__init__()
            self.entered = Event()
            self.release = Event()
            self.armed = False
            self.failure = None

        def update(self, docs, key=None, **kwargs):
            if self.armed:
                self.entered.set()
                if self.failure is not None:
                    raise self.failure
                if not self.release.wait(30):
                    raise RuntimeError("payload update release timed out")
            return super().update(docs, key=key, **kwargs)

    return ControlledMemoryStore()


def _start_paused_payload_write(store, payload_store, document, save):
    from threading import Thread

    failures = []

    def write():
        try:
            store.update(document, save=save)
        except BaseException as error:
            failures.append(error)

    worker = Thread(target=write)
    worker.start()
    if not payload_store.entered.wait(10):
        payload_store.release.set()
        worker.join(10)
        raise AssertionError(
            "payload update was not reached; "
            f"worker alive: {worker.is_alive()}; worker failures: {failures!r}"
        )
    return worker, failures


def _release_paused_payload_write(payload_store, worker, failures):
    payload_store.release.set()
    worker.join(10)
    assert not worker.is_alive(), f"worker failures: {failures!r}"
    assert failures == []


@pytest.fixture
def payload_publication_source_identity(request, capsys):
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
    record = json.dumps(metadata, sort_keys=True)
    with capsys.disabled():
        print(f"JOBFLOW_PAYLOAD_PUBLICATION_RUNTIME {record}")


def test_payload_publication_hides_new_document_until_payload_update_returns(
    payload_publication_source_identity,
):
    uuid = "payload-publication-job"
    payload_store = _make_controlled_payload_store()
    store, _, _ = _payload_publication_jobstore(payload_store)
    _seed_payload_publication_index(store, uuid)
    payload_store.armed = True
    payload = _payload_publication_output("index-two", [13, 21, 34])
    document = {
        "uuid": uuid,
        "index": 2,
        "sentinel": "preserve-index-two",
        "output": payload,
    }
    worker, failures = _start_paused_payload_write(
        store, payload_store, document, {"data": "output"}
    )
    try:
        raw = store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None)
        assert raw is None
    finally:
        _release_paused_payload_write(payload_store, worker, failures)

    raw = store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None)
    assert raw["sentinel"] == "preserve-index-two"
    assert store.get_output(uuid, which="last", load=True) == payload


def test_latest_output_remains_index_one_while_payload_update_is_paused(
    payload_publication_source_identity,
):
    uuid = "payload-publication-job"
    payload_store = _make_controlled_payload_store()
    store, _, _ = _payload_publication_jobstore(payload_store)
    first_payload = _seed_payload_publication_index(store, uuid)
    payload_store.armed = True
    document = {
        "uuid": uuid,
        "index": 2,
        "sentinel": "preserve-index-two",
        "output": _payload_publication_output("index-two", [13, 21, 34]),
    }
    worker, failures = _start_paused_payload_write(
        store, payload_store, document, {"data": "output"}
    )
    try:
        assert store.get_output(uuid, which="last", load=True) == first_payload
    finally:
        _release_paused_payload_write(payload_store, worker, failures)


def test_payload_failure_propagates_before_document_publication(
    payload_publication_source_identity,
):
    import pytest

    uuid = "payload-publication-job"
    payload_store = _make_controlled_payload_store()
    store, _, _ = _payload_publication_jobstore(payload_store)
    _seed_payload_publication_index(store, uuid)
    failure = RuntimeError("payload update failed before persistence")
    payload_store.failure = failure
    payload_store.armed = True
    document = {
        "uuid": uuid,
        "index": 2,
        "sentinel": "preserve-index-two",
        "output": _payload_publication_output("index-two", [13, 21, 34]),
    }
    with pytest.raises(
        RuntimeError, match="payload update failed before persistence"
    ) as exc:
        store.update(document, save={"data": "output"})
    assert exc.value is failure
    assert (
        store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None) is None
    )
    assert (
        list(payload_store.query({"job_uuid": uuid, "job_index": 2}, properties=None))
        == []
    )


def test_latest_output_remains_index_one_after_payload_failure(
    payload_publication_source_identity,
):
    import pytest

    uuid = "payload-publication-job"
    payload_store = _make_controlled_payload_store()
    store, _, _ = _payload_publication_jobstore(payload_store)
    first_payload = _seed_payload_publication_index(store, uuid)
    failure = RuntimeError("payload update failed before persistence")
    payload_store.failure = failure
    payload_store.armed = True
    document = {
        "uuid": uuid,
        "index": 2,
        "sentinel": "preserve-index-two",
        "output": _payload_publication_output("index-two", [13, 21, 34]),
    }
    with pytest.raises(RuntimeError) as exc:
        store.update(document, save={"data": "output"})
    assert exc.value is failure
    assert store.get_output(uuid, which="last", load=True) == first_payload


def test_payload_publication_succeeds_after_payload_is_saved(
    payload_publication_source_identity,
):
    uuid = "payload-publication-job"
    store, payload_store, _ = _payload_publication_jobstore()
    _seed_payload_publication_index(store, uuid)
    payload = _payload_publication_output("index-two", [13, 21, 34])
    store.update(
        {
            "uuid": uuid,
            "index": 2,
            "sentinel": "preserve-index-two",
            "output": payload,
        },
        save={"data": "output"},
    )
    raw = store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None)
    placeholder = raw["output"]
    assert raw["sentinel"] == "preserve-index-two"
    assert placeholder["store"] == "data"
    assert placeholder["blob_uuid"]
    records = list(
        payload_store.query({"blob_uuid": placeholder["blob_uuid"]}, properties=None)
    )
    assert len(records) == 1
    assert records[0]["data"] == payload
    assert records[0]["job_uuid"] == uuid
    assert records[0]["job_index"] == 2
    assert store.get_output(uuid, which="last", load=True) == payload


def test_save_false_keeps_output_in_document_store(
    payload_publication_source_identity,
):
    uuid = "payload-publication-job"
    store, payload_store, _ = _payload_publication_jobstore()
    _seed_payload_publication_index(store, uuid)
    payload = _payload_publication_output("index-two", [13, 21, 34])
    store.update(
        {
            "uuid": uuid,
            "index": 2,
            "sentinel": "preserve-index-two",
            "output": payload,
        },
        save=False,
    )
    raw = store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None)
    assert raw["sentinel"] == "preserve-index-two"
    assert raw["output"] == payload
    assert (
        list(payload_store.query({"job_uuid": uuid, "job_index": 2}, properties=None))
        == []
    )
    assert store.get_output(uuid, which="last", load=True) == payload


def test_unmatched_save_selector_keeps_output_in_document_store(
    payload_publication_source_identity,
):
    uuid = "payload-publication-job"
    store, payload_store, _ = _payload_publication_jobstore()
    _seed_payload_publication_index(store, uuid)
    payload = _payload_publication_output("index-two", [13, 21, 34])
    store.update(
        {
            "uuid": uuid,
            "index": 2,
            "sentinel": "preserve-index-two",
            "output": payload,
        },
        save={"data": "missing"},
    )
    raw = store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None)
    assert raw["sentinel"] == "preserve-index-two"
    assert raw["output"] == payload
    assert (
        list(payload_store.query({"job_uuid": uuid, "job_index": 2}, properties=None))
        == []
    )
    assert store.get_output(uuid, which="last", load=True) == payload


def test_default_created_additional_store_is_connected(
    payload_publication_source_identity,
):
    from collections import defaultdict

    from maggma.stores import MemoryStore

    from jobflow import JobStore

    uuid = "payload-publication-job"
    docs_store = MemoryStore()
    docs_store.connect()

    def connected_store():
        result = MemoryStore()
        result.connect()
        return result

    additional_stores = defaultdict(connected_store)
    store = JobStore(docs_store, additional_stores=additional_stores)
    store.update(
        {
            "uuid": uuid,
            "index": 1,
            "sentinel": "preserve-index-one",
            "output": _payload_publication_output("index-one", [3, 5, 8]),
        },
        save=False,
    )
    payload = _payload_publication_output("index-two", [13, 21, 34])
    store.update(
        {
            "uuid": uuid,
            "index": 2,
            "sentinel": "preserve-index-two",
            "output": payload,
        },
        save={"data": "output"},
    )
    payload_store = additional_stores["data"]
    raw = store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None)
    placeholder = raw["output"]
    assert placeholder["store"] == "data"
    assert placeholder["blob_uuid"]
    records = list(
        payload_store.query({"blob_uuid": placeholder["blob_uuid"]}, properties=None)
    )
    assert len(records) == 1
    assert records[0]["data"] == payload
    assert records[0]["job_uuid"] == uuid
    assert records[0]["job_index"] == 2
    assert store.get_output(uuid, which="last", load=True) == payload


def test_document_failure_preserves_exception_and_reports_orphan_payloads(
    payload_publication_source_identity, capsys
):
    from maggma.stores import MemoryStore

    class FailingDocumentMemoryStore(MemoryStore):
        def __init__(self):
            super().__init__()
            self.armed = False
            self.failure = None

        def update(self, docs, key=None, **kwargs):
            if self.armed and any(doc.get("index") == 2 for doc in docs):
                raise self.failure
            return super().update(docs, key=key, **kwargs)

    uuid = "payload-publication-job"
    docs_store = FailingDocumentMemoryStore()
    store, payload_store, _ = _payload_publication_jobstore(docs_store=docs_store)
    first_payload = _seed_payload_publication_index(store, uuid)
    failure = RuntimeError("document update failed")
    docs_store.failure = failure
    docs_store.armed = True
    payload = _payload_publication_output("index-two", [13, 21, 34])
    with pytest.raises(RuntimeError, match="document update failed") as exc:
        store.update(
            {
                "uuid": uuid,
                "index": 2,
                "sentinel": "preserve-index-two",
                "output": payload,
            },
            save={"data": "output"},
        )
    assert exc.value is failure
    raw = store.query_one({"uuid": uuid}, load=False, properties=None)
    assert raw["index"] == 1
    assert raw["sentinel"] == "preserve-index-one"
    assert (
        store.query_one({"uuid": uuid, "index": 2}, load=False, properties=None) is None
    )
    assert store.get_output(uuid, which="last", load=True) == first_payload
    orphan_records = list(
        payload_store.query({"job_uuid": uuid, "job_index": 2}, properties=None)
    )
    for record in orphan_records:
        assert record["job_uuid"] == uuid
        assert record["job_index"] == 2
        assert record["data"] == payload
    orphan_count = len(
        list(payload_store.query({"job_uuid": uuid, "job_index": 2}, properties=None))
    )
    with capsys.disabled():
        print(f"JOBFLOW_PAYLOAD_PUBLICATION_ORPHAN_COUNT {orphan_count}")


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
