from datetime import datetime

import pytest

from jobflow import JobStore
from jobflow.schemas.job_store import JobStoreDocument


@pytest.fixture
def memory_store():
    from maggma.stores import MemoryStore

    store = MemoryStore()
    store.connect()
    return store


@pytest.fixture
def sample_data():
    return JobStoreDocument(
        uuid="abc123",
        index=1,
        output=None,
        completed_at=datetime.now().isoformat(),
        metadata={"key": "value"},
        hosts=["host1", "host2"],
        name="my_job",
    )


def test_job_store_document_model(sample_data):
    # Test creating model
    data = sample_data

    assert data.uuid == "abc123"
    assert data.index == 1
    assert data.output is None
    assert datetime.fromisoformat(data.completed_at).hour == datetime.now().hour
    assert data.metadata == {"key": "value"}
    assert data.hosts == ["host1", "host2"]
    assert data.name == "my_job"


def test_job_store_update(memory_store, sample_data):
    # Storing document as a JobStoreDocument
    store = JobStore(memory_store)
    store.connect()
    d = {
        "index": 1,
        "uuid": "abc123",
        "metadata": {"key": "value"},
        "hosts": ["host1", "host2"],
        "name": "my_job",
        "e": 6,
        "d": 4,
    }
    sample_data = JobStoreDocument(**d)
    store.update(sample_data)

    # Check document was inserted
    results = store.query_one(criteria={"hosts": {"$exists": 1}})
    assert results["index"] == 1
    assert results["uuid"] == "abc123"
    assert results["metadata"] == {"key": "value"}
    assert results["hosts"] == ["host1", "host2"]
    assert results["name"] == "my_job"
    assert "e" not in results
    assert "d" not in results

    # Further checks to see if two documents get inserted
    e = d.copy()
    e["uuid"] = "def456"
    new_data_e = JobStoreDocument(**e)
    f = d.copy()
    f["uuid"] = "ghi789"
    new_data_f = JobStoreDocument(**f)
    store.update([new_data_e, new_data_f])

    # Check if document new_data_e is present in the store
    results = store.query_one(criteria={"uuid": "def456"})
    assert results["index"] == 1
    assert results["uuid"] == "def456"
    assert results["metadata"] == {"key": "value"}
    assert results["hosts"] == ["host1", "host2"]
    assert results["name"] == "my_job"
    assert "e" not in results
    assert "d" not in results

    # Check if document new_data_f is present in the store
    results = store.query_one(criteria={"uuid": "ghi789"})
    assert results["index"] == 1
    assert results["uuid"] == "ghi789"
    assert results["metadata"] == {"key": "value"}
    assert results["hosts"] == ["host1", "host2"]
    assert results["name"] == "my_job"
    assert "e" not in results
    assert "d" not in results
