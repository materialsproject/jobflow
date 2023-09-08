from datetime import datetime

import pytest


@pytest.fixture
def sample_data():
    from jobflow.schemas.job_output_schema import JobStoreDocument

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


def test_job_store_update(memory_jobstore, sample_data):
    # Storing document as a JobStoreDocument
    from jobflow.schemas.job_output_schema import JobStoreDocument

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
    memory_jobstore.update(sample_data)

    # Check document was inserted
    results = memory_jobstore.query_one(criteria={"hosts": {"$exists": 1}})
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
    memory_jobstore.update([new_data_e, new_data_f])

    # Check if document new_data_e is present in the store
    results = memory_jobstore.query_one(criteria={"uuid": "def456"})
    assert results["index"] == 1
    assert results["uuid"] == "def456"
    assert results["metadata"] == {"key": "value"}
    assert results["hosts"] == ["host1", "host2"]
    assert results["name"] == "my_job"
    assert "e" not in results
    assert "d" not in results

    # Check if document new_data_f is present in the store
    results = memory_jobstore.query_one(criteria={"uuid": "ghi789"})
    assert results["index"] == 1
    assert results["uuid"] == "ghi789"
    assert results["metadata"] == {"key": "value"}
    assert results["hosts"] == ["host1", "host2"]
    assert results["name"] == "my_job"
    assert "e" not in results
    assert "d" not in results
