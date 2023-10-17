from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("jobflow")
except PackageNotFoundError:
    # package is not installed
    __version__ = ""
