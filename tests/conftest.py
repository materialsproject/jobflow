import pytest


@pytest.fixture(scope="session")
def test_database():
    return "activities_test"


@pytest.fixture(scope="session")
def output_store(test_database):
    from maggma.stores import MongoStore
    return MongoStore(test_database, "activity_outputs")
