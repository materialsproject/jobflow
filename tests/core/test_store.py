from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

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


def _projection_documents():
    return [
        {
            "uuid": "projection-1",
            "index": 1,
            "keep": "first",
            "hidden": "secret-1",
            "sentinel": "sentinel-1",
            "group": "selected",
            "rank": 2,
            "output": {"small": "visible", "large": "large-1"},
            "items": [
                {"kind": "match", "value": 1},
                {"kind": "other", "value": 2},
                {"kind": "match", "value": 3},
            ],
        },
        {
            "uuid": "projection-2",
            "index": 1,
            "keep": "second",
            "hidden": "secret-2",
            "sentinel": "sentinel-2",
            "group": "selected",
            "rank": 1,
            "output": {"small": "visible", "large": "large-2"},
            "items": [
                {"kind": "other", "value": 4},
                {"kind": "match", "value": 5},
            ],
        },
        {
            "uuid": "projection-3",
            "index": 1,
            "keep": "third",
            "hidden": "secret-3",
            "sentinel": "sentinel-3",
            "group": "other",
            "rank": 3,
            "output": {"small": "visible", "large": "large-3"},
            "items": [{"kind": "other", "value": 6}],
        },
    ]


def _seed_projection_store(store):
    documents = _projection_documents()
    store.update(documents)
    return documents


@pytest.fixture
def mongo_projection_jobstore(database, request, capsys):
    import inspect
    import json

    from maggma.stores import MemoryStore, MongoStore

    from jobflow import JobStore

    projection_database = f"{database}_projection_{uuid4().hex}"
    collection_name = "outputs"
    docs_store = MongoStore(projection_database, collection_name)
    store = JobStore(docs_store, additional_stores={"data": MemoryStore()})
    store.connect()
    try:
        source_file = Path(inspect.getsourcefile(JobStore)).resolve()
        checkout_source = (
            Path(__file__).resolve().parents[2] / "src/jobflow/core/store.py"
        )
        loaded_digest = sha256(source_file.read_bytes()).hexdigest()
        checkout_digest = sha256(checkout_source.read_bytes()).hexdigest()
        mongo_client = docs_store._collection.database.client
        mongo_client_type = type(mongo_client)
        mongo_client_name = (
            f"{mongo_client_type.__module__}.{mongo_client_type.__name__}"
        )
        mongo_version = mongo_client.server_info()["version"]
        metadata = {
            "mongo_database": projection_database,
            "mongo_collection": collection_name,
            "mongo_client_type": mongo_client_name,
            "jobstore_source": str(source_file),
            "jobstore_source_sha256": loaded_digest,
            "mongo_server_version": mongo_version,
            "pytest_nodeid": request.node.nodeid,
        }
        with capsys.disabled():
            print(
                "JOBFLOW_PROJECTION_RUNTIME " + json.dumps(metadata, sort_keys=True),
                flush=True,
            )
        assert loaded_digest == checkout_digest, metadata
        assert mongo_client_type.__module__.startswith("pymongo."), metadata
        yield store
    finally:
        docs_store._collection.drop()
        store.close()


def test_query_nested_exclusion_matches_backend(memory_jobstore):
    _seed_projection_store(memory_jobstore)
    properties = {"output.large": 0}
    expected = list(memory_jobstore.docs_store.query(properties=deepcopy(properties)))

    actual = list(memory_jobstore.query(properties=properties))

    assert actual == expected

    assert all("large" not in document["output"] for document in actual)


def test_query_one_nested_exclusion_matches_backend(memory_jobstore):
    _seed_projection_store(memory_jobstore)
    properties = {"output.large": 0}
    expected = next(
        memory_jobstore.docs_store.query(
            criteria={"uuid": "projection-1"}, properties=deepcopy(properties)
        )
    )

    actual = memory_jobstore.query_one(
        criteria={"uuid": "projection-1"}, properties=properties
    )

    assert actual == expected
    assert "large" not in actual["output"]


def test_query_reuses_caller_list_without_mutation(memory_jobstore):
    _seed_projection_store(memory_jobstore)
    properties = ["keep"]
    expected = list(
        memory_jobstore.docs_store.query(properties=["keep", "uuid", "index"])
    )

    for _ in range(2):
        assert list(memory_jobstore.query(properties=properties)) == expected

    assert properties == ["keep"]


def test_query_reuses_caller_mapping_without_mutation(memory_jobstore):
    _seed_projection_store(memory_jobstore)
    properties = {"keep": 1}
    expected = list(
        memory_jobstore.docs_store.query(properties={"keep": 1, "uuid": 1, "index": 1})
    )

    for _ in range(2):
        assert list(memory_jobstore.query(properties=properties)) == expected

    assert properties == {"keep": 1}


def test_query_one_reuses_caller_mapping_without_mutation(memory_jobstore):
    _seed_projection_store(memory_jobstore)
    properties = {"keep": 1}
    expected = next(
        memory_jobstore.docs_store.query(
            criteria={"uuid": "projection-1"},
            properties={"keep": 1, "uuid": 1, "index": 1},
        )
    )

    for _ in range(2):
        assert (
            memory_jobstore.query_one(
                criteria={"uuid": "projection-1"}, properties=properties
            )
            == expected
        )

    assert properties == {"keep": 1}


def test_query_does_not_mutate_properties_when_backend_iteration_fails(
    memory_jobstore,
):
    _seed_projection_store(memory_jobstore)
    properties = {"keep": 1, "hidden": 0}
    backend_properties = deepcopy(properties)
    with pytest.raises(
        ValueError, match=r"You cannot currently mix including and excluding fields\."
    ) as backend_error:
        list(memory_jobstore.docs_store.query(properties=backend_properties))

    assert backend_properties == properties

    with pytest.raises(type(backend_error.value)) as jobstore_error:
        list(memory_jobstore.query(properties=properties))

    assert type(jobstore_error.value) is type(backend_error.value)
    assert str(jobstore_error.value) == str(backend_error.value)
    assert properties == {"keep": 1, "hidden": 0}


@pytest.mark.parametrize("properties", [(), ("keep",)])
def test_query_accepts_tuple_projection(memory_jobstore, properties):
    _seed_projection_store(memory_jobstore)
    fields = [*properties, "uuid", "index"]
    expected = list(memory_jobstore.docs_store.query(properties=fields))

    actual = list(memory_jobstore.query(properties=properties))

    assert actual == expected


@pytest.mark.parametrize("properties", [(), ("keep",)])
def test_query_one_accepts_tuple_projection(memory_jobstore, properties):
    _seed_projection_store(memory_jobstore)
    fields = [*properties, "uuid", "index"]
    expected = next(
        memory_jobstore.docs_store.query(
            criteria={"uuid": "projection-1"}, properties=fields
        )
    )

    actual = memory_jobstore.query_one(
        criteria={"uuid": "projection-1"}, properties=properties
    )

    assert actual == expected


@pytest.mark.parametrize(
    ("properties", "reference_properties", "unrestricted"),
    [
        (None, None, True),
        ({}, {}, True),
        ([], ["uuid", "index"], False),
        ((), ["uuid", "index"], False),
    ],
)
def test_query_empty_and_unrestricted_projection_shapes(
    memory_jobstore, properties, reference_properties, unrestricted
):
    documents = _seed_projection_store(memory_jobstore)
    expected = list(memory_jobstore.docs_store.query(properties=reference_properties))

    actual = list(memory_jobstore.query(properties=properties))

    assert actual == expected
    if unrestricted:
        assert all("sentinel" in document for document in actual)
    else:
        assert all(set(document) <= {"_id", "uuid", "index"} for document in actual)
    assert len(actual) == len(documents)


@pytest.mark.parametrize("properties", [["keep"], {"keep": 1}])
def test_query_inclusion_projection_matches_backend(memory_jobstore, properties):
    _seed_projection_store(memory_jobstore)
    if isinstance(properties, list):
        reference_properties = ["keep", "uuid", "index"]
    else:
        reference_properties = {"keep": 1, "uuid": 1, "index": 1}
    expected = list(memory_jobstore.docs_store.query(properties=reference_properties))

    actual = list(memory_jobstore.query(properties=deepcopy(properties)))

    assert actual == expected
    assert all("sentinel" not in document for document in actual)


def test_query_exclusion_projection_preserves_other_fields(memory_jobstore):
    _seed_projection_store(memory_jobstore)
    properties = {"hidden": 0}
    expected = list(memory_jobstore.docs_store.query(properties=deepcopy(properties)))

    actual = list(memory_jobstore.query(properties=properties))

    assert actual == expected
    assert all(
        "sentinel" in document and "hidden" not in document for document in actual
    )


def test_query_exclusions_preserve_required_identifiers(memory_jobstore):
    documents = _seed_projection_store(memory_jobstore)
    properties = {"uuid": 0, "index": 0, "hidden": 0}
    backend_results = list(
        memory_jobstore.docs_store.query(properties=deepcopy(properties))
    )
    by_sentinel = {document["sentinel"]: document for document in documents}
    expected = []
    for document in backend_results:
        result = deepcopy(document)
        original = by_sentinel[result["sentinel"]]
        result.update(uuid=original["uuid"], index=original["index"])
        expected.append(result)

    actual = list(memory_jobstore.query(properties=deepcopy(properties)))

    assert actual == expected
    assert all("uuid" in document and "index" in document for document in actual)
    assert all("hidden" not in document for document in actual)


@pytest.mark.parametrize(
    ("properties", "reference_properties", "expected_fields"),
    [
        ({"_id": 1}, {"_id": 1, "uuid": 1, "index": 1}, None),
        ({"_id": 0}, {"_id": 0}, None),
        (
            {"keep": 1, "_id": 0},
            {"keep": 1, "_id": 0, "uuid": 1, "index": 1},
            {"keep", "uuid", "index"},
        ),
        ({"hidden": 0, "_id": 1}, {"hidden": 0, "_id": 1}, None),
    ],
)
def test_query_preserves_id_projection_modes(
    memory_jobstore, properties, reference_properties, expected_fields
):
    _seed_projection_store(memory_jobstore)
    expected = list(memory_jobstore.docs_store.query(properties=reference_properties))

    actual = list(memory_jobstore.query(properties=deepcopy(properties)))

    assert actual == expected
    if expected_fields is not None:
        assert all(set(document) - {"_id"} == expected_fields for document in actual)


def test_query_forwards_criteria_sort_skip_and_limit(memory_jobstore):
    _seed_projection_store(memory_jobstore)

    actual = list(
        memory_jobstore.query(
            criteria={"group": "selected"},
            properties=["keep"],
            sort={"rank": 1},
            skip=1,
            limit=1,
        )
    )

    assert [document["uuid"] for document in actual] == ["projection-1"]
    assert [document["keep"] for document in actual] == ["first"]


def test_query_one_forwards_criteria_and_sort(memory_jobstore):
    _seed_projection_store(memory_jobstore)

    actual = memory_jobstore.query_one(
        criteria={"group": "selected"},
        properties=["keep"],
        sort={"rank": -1},
    )

    assert actual["uuid"] == "projection-1"
    assert actual["keep"] == "first"


def test_query_projection_loads_nested_blob(memory_data_jobstore):
    document = {
        "uuid": "blob-projection",
        "index": 1,
        "sentinel": "blob-sentinel",
        "output": {"small": "visible", "large": {"values": [1, 2, 3]}},
    }
    memory_data_jobstore.update(document, save={"data": [["output", "large"]]})

    placeholder = memory_data_jobstore.query_one(
        criteria={"uuid": document["uuid"]},
        properties=["output"],
        load=False,
    )
    loaded = memory_data_jobstore.query_one(
        criteria={"uuid": document["uuid"]},
        properties=["output"],
        load=True,
    )

    assert placeholder["output"]["large"]["store"] == "data"
    assert placeholder["output"]["large"]["blob_uuid"]
    assert loaded["output"] == document["output"]


def test_query_excluded_blob_stays_absent_when_loading(memory_data_jobstore):
    document = {
        "uuid": "excluded-blob-projection",
        "index": 1,
        "sentinel": "excluded-blob-sentinel",
        "output": {"small": "visible", "large": {"values": [1, 2, 3]}},
    }
    memory_data_jobstore.update(document, save={"data": [["output", "large"]]})

    actual = memory_data_jobstore.query_one(
        criteria={"uuid": document["uuid"]},
        properties={"output.large": 0},
        load=True,
    )

    assert actual["output"] == {"small": "visible"}
    assert "large" not in actual["output"]


@pytest.mark.parametrize(
    ("properties", "reference_properties", "mode"),
    [
        ({"items": {"$slice": 2}}, {"items": {"$slice": 2}}, "slice"),
        (
            {"hidden": 0, "items": {"$slice": 2}},
            {"hidden": 0, "items": {"$slice": 2}},
            "exclude_slice",
        ),
        (
            {"keep": 1, "items": {"$slice": 2}},
            {"keep": 1, "items": {"$slice": 2}, "uuid": 1, "index": 1},
            "include_slice",
        ),
        (
            {"items": {"$elemMatch": {"kind": "match"}}},
            {"items": {"$elemMatch": {"kind": "match"}}, "uuid": 1, "index": 1},
            "elem_match",
        ),
        (
            {"items": {"$elemMatch": {"kind": "match"}}, "uuid": 0},
            {"items": {"$elemMatch": {"kind": "match"}}, "uuid": 1, "index": 1},
            "elem_match",
        ),
        (
            {"uuid": 1, "items": {"$slice": 1}},
            {"uuid": 1, "index": 1, "items": {"$slice": 1}},
            "include_identifier_slice",
        ),
    ],
)
@pytest.mark.parametrize("operation", ["query", "query_one"])
def test_mongo_projection_operators(
    mongo_projection_jobstore, properties, reference_properties, mode, operation
):
    _seed_projection_store(mongo_projection_jobstore)
    criteria = {"uuid": "projection-1"}
    expected = list(
        mongo_projection_jobstore.docs_store.query(
            criteria=criteria, properties=deepcopy(reference_properties), limit=1
        )
    )
    assert len(expected) == 1
    expected_document = expected[0]
    if mode == "slice":
        assert expected_document["items"] == _projection_documents()[0]["items"][:2]
        assert expected_document["hidden"] == "secret-1"
    elif mode == "exclude_slice":
        assert expected_document["items"] == _projection_documents()[0]["items"][:2]
        assert "hidden" not in expected_document
        assert expected_document["sentinel"] == "sentinel-1"
    elif mode == "include_slice":
        assert set(expected_document) == {"_id", "uuid", "index", "keep", "items"}
        assert expected_document["items"] == _projection_documents()[0]["items"][:2]
    elif mode == "include_identifier_slice":
        assert set(expected_document) == {"_id", "uuid", "index", "items"}
        assert expected_document["items"] == _projection_documents()[0]["items"][:1]
    else:
        assert set(expected_document) == {"_id", "uuid", "index", "items"}
        assert expected_document["items"] == [_projection_documents()[0]["items"][0]]

    if operation == "query":
        actual = list(
            mongo_projection_jobstore.query(
                criteria=criteria, properties=deepcopy(properties)
            )
        )
    else:
        actual = [
            mongo_projection_jobstore.query_one(
                criteria=criteria, properties=deepcopy(properties)
            )
        ]

    assert actual == expected


@pytest.mark.parametrize("operation", ["query", "query_one"])
def test_mongo_id_meta_projection_preserves_other_fields(
    mongo_projection_jobstore, operation
):
    _seed_projection_store(mongo_projection_jobstore)
    mongo_projection_jobstore.docs_store._collection.create_index(
        [("keep", "text")], default_language="none"
    )
    criteria = {"$text": {"$search": "first"}}
    properties = {"_id": {"$meta": "textScore"}}
    snapshot = deepcopy(properties)
    expected = list(
        mongo_projection_jobstore.docs_store.query(
            criteria=criteria, properties=deepcopy(properties)
        )
    )
    assert len(expected) == 1
    assert expected[0]["uuid"] == "projection-1"
    assert expected[0]["index"] == 1
    assert expected[0]["sentinel"] == "sentinel-1"
    assert expected[0]["hidden"] == "secret-1"

    if operation == "query":
        actual = list(
            mongo_projection_jobstore.query(criteria=criteria, properties=properties)
        )
    else:
        actual = [
            mongo_projection_jobstore.query_one(
                criteria=criteria, properties=properties
            )
        ]

    assert actual == expected
    assert properties == snapshot
