import pytest


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
    assert store.collection is not None

    store.close()

    store = JobStore(memory_store, load=None)
    store.connect()
    assert store


def test_additional(memory_store):
    from copy import deepcopy

    from jobflow import JobStore

    store = JobStore(memory_store, additional_stores={"data": deepcopy(memory_store)})
    store.connect()
    assert store
    assert store.name == "JobStore-mem://memory_db"
    assert store.collection is not None

    store.close()


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

    results = memory_data_jobstore.query_one(c, load={"data": False})
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

    # test bad store name fails
    results["data"]["store"] = "bad_store"
    memory_data_jobstore.update(results)
    with pytest.raises(ValueError):
        memory_data_jobstore.query_one(c, load=True)

    with pytest.raises(ValueError):
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
    grouped_by_9 = [g[1] for g in data if g[0]["d"] == 9][0]
    assert len(grouped_by_9) == 3
    grouped_by_10 = [g[1] for g in data if g[0]["d"] == 10][0]
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

    with pytest.raises(ValueError):
        memory_jobstore.get_output(1, which="first")

    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
        JobStore.from_file(test_data / "db_bad.yaml")


def test_ensure_index(memory_jobstore):
    assert memory_jobstore.ensure_index("test_key")
    # TODO: How to check for exception?
