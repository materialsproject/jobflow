import pytest


@pytest.fixture
def memory_store():
    from maggma.stores import MemoryStore

    store = MemoryStore()
    store.connect()
    return store


def test_jobstore_connect(memory_store):
    from jobflow import JobStore

    store = JobStore(memory_store)
    store.connect()
    assert store


def test_jobstore_doc_update(memory_jobstore):
    # all standard mongo updates should work fine
    d = {"index": 1, "uuid": 1, "e": 6, "d": 4}
    memory_jobstore.update(d)
    results = memory_jobstore.query_one(criteria={"d": {"$exists": 1}})
    assert results["d"] == 4

    d = [{"index": 1, "uuid": 2, "e": 7, "d": 8, "f": 9}]
    memory_jobstore.update(d, key=["d", "f"])
    result = memory_jobstore.query_one(criteria={"d": 8, "f": 9}, properties=["e"])
    assert result["e"] == 7

    memory_jobstore.update({"e": "abc", "d": 3, "index": 1, "uuid": 5}, key="e")


def test_jobstore_data_update(memory_data_jobstore):
    d = {"index": 1, "uuid": 1, "e": 6, "d": 4, "data": [1, 2, 3, 4]}
    memory_data_jobstore.update(d, save={"data": "data"})

    c = {"d": {"$exists": 1}}
    results = memory_data_jobstore.query_one(c, load={"data": "data"})
    assert results["data"] == [1, 2, 3, 4]

    c = {"d": {"$exists": 1}}
    results = memory_data_jobstore.query_one(c, load={"data": ["data"]})
    assert results["data"] == [1, 2, 3, 4]

    results = memory_data_jobstore.query_one(c, load={"data": True})
    assert results["data"] == [1, 2, 3, 4]

    results = memory_data_jobstore.query_one(c, load=True)
    assert results["data"] == [1, 2, 3, 4]

    results = memory_data_jobstore.query_one(c, load={})
    assert type(results["data"]) == dict
    assert "@class" in results["data"]
    assert "@module" in results["data"]
    assert "blob_uuid" in results["data"]

    results = memory_data_jobstore.query_one(c, load=False)
    assert type(results["data"]) == dict
    assert "@class" in results["data"]
    assert "@module" in results["data"]
    assert "blob_uuid" in results["data"]

    results = memory_data_jobstore.query_one(c, load=None)
    assert type(results["data"]) == dict
    assert "@class" in results["data"]
    assert "@module" in results["data"]
    assert "blob_uuid" in results["data"]


def test_jobstore_count(memory_jobstore):
    d = {"index": 1, "uuid": 1, "a": 1, "b": 2, "c": 3, "data": [1, 2, 3, 4]}
    memory_jobstore.update(d)
    assert memory_jobstore.count() == 1

    d = {"index": 1, "uuid": 2, "aa": 1, "b": 2, "c": 3, "data": [1, 2]}
    memory_jobstore.update(d)
    assert memory_jobstore.count() == 2
    assert memory_jobstore.count({"a": 1}) == 1


def test_jobstore_distinct(memory_jobstore):
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


def test_jobstore_groupby(memory_jobstore):
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
    grouped_by_9 = [g[1] for g in data if g[0]["d"] == 9][0]
    assert len(grouped_by_9) == 3
    grouped_by_10 = [g[1] for g in data if g[0]["d"] == 10][0]
    assert len(grouped_by_10) == 1

    data = list(memory_jobstore.groupby(["e", "d"]))
    assert len(data) == 3


def test_jobstore_remove_docs(memory_jobstore):
    d1 = {"a": 1, "b": 2, "c": 3, "index": 1, "uuid": 1}
    d2 = {"a": 4, "d": 5, "e": 6, "g": {"h": 1}, "index": 1, "uuid": 2}
    memory_jobstore.update(d1)
    memory_jobstore.update(d2)
    memory_jobstore.remove_docs({"a": 1})
    assert len(list(memory_jobstore.query({"a": 4}))) == 1
    assert len(list(memory_jobstore.query({"a": 1}))) == 0


def test_jobstore_from_db_file(test_data):
    from jobflow import JobStore

    ms = JobStore.from_file(test_data / "db.yaml")
    ms.connect()
    assert ms.docs_store.name == "mongo://localhost/jobflow_unittest/outputs"
    assert ms.additional_stores == {}


def test_jobstore_from_db_file_s3(test_data):
    from jobflow import JobStore

    ms = JobStore.from_file(test_data / "db_gridfs.yaml")
    ms.connect()
    data_store = ms.additional_stores["data"]
    assert ms.docs_store.name == "mongo://localhost/jobflow_unittest/outputs"
    assert data_store.name() == "gridfs://localhost/jobflow_unittest/outputs_blobs"


def test_ensure_index(memory_jobstore):
    assert memory_jobstore.ensure_index("test_key")
    # TODO: How to check for exception?


def test_jobstore_last_updated(memory_jobstore):
    from datetime import datetime

    from maggma.core import StoreError

    assert memory_jobstore.last_updated == datetime.min

    start_time = datetime.utcnow()
    memory_jobstore.update({"uuid": 1, "index": 1, "a": 1})
    with pytest.raises(StoreError) as cm:
        _ = memory_jobstore.last_updated

    assert cm.match(memory_jobstore.last_updated_field)
    lu_field = memory_jobstore.last_updated_field
    d = {"uuid": 2, "index": 1, "a": 1, lu_field: datetime.utcnow()}
    memory_jobstore.update([d])
    assert memory_jobstore.last_updated > start_time


def test_job_store_newer_in(memory_jobstore, memory_store):
    from datetime import datetime

    from jobflow import JobStore

    target = JobStore(memory_store)
    target.connect()

    lu_field = memory_jobstore.last_updated_field

    # make sure docs are newer in job_store then target and check updated_keys
    docs = [{"uuid": i, "index": i, lu_field: datetime.utcnow()} for i in range(10)]
    target.update(docs)

    # Update docs in source
    docs = [{"uuid": i, "index": i, lu_field: datetime.utcnow()} for i in range(10)]
    memory_jobstore.update(docs)

    assert len(target.newer_in(memory_jobstore)) == 10
    assert len(target.newer_in(memory_jobstore, exhaustive=True)) == 10
    assert len(memory_jobstore.newer_in(target)) == 0
