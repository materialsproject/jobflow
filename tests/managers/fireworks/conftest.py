import pytest


@pytest.fixture(scope="session")
def lpad(test_database):
    from fireworks import LaunchPad
    lpad = LaunchPad(name=test_database)
    lpad.reset("", require_password=False)
    return lpad
