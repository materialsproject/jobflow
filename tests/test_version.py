import re


def test_installed_version():
    import jobflow._version
    from jobflow import __version__

    assert re.match(r"^\d+\.\d+\.\d+.*", __version__)
    assert __version__ == jobflow._version.__version__
