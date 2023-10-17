import re

import pytest


@pytest.fixture
def uninstall_jobflow(monkeypatch):
    import pkg_resources

    original_func = pkg_resources.get_distribution

    def mock_func(name):
        if name == "jobflow":
            raise pkg_resources.DistributionNotFound()
        return original_func(name)

    monkeypatch.setattr(pkg_resources, "get_distribution", mock_func)


def test_installed_version():
    from jobflow import __version__

    assert re.match(r"^\d+\.\d+\.\d+$", __version__)


def test_not_installed_version(uninstall_jobflow):
    import importlib

    from jobflow import _version

    # reimport modules
    importlib.reload(_version)

    # test version
    assert _version.__version__ == ""
