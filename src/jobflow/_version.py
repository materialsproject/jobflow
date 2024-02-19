from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("jobflow")
except PackageNotFoundError:  # pragma: no cover
    # package is not installed
    __version__ = ""
