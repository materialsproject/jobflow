"""Jobflow is a package for writing dynamic and connected workflows."""

from jobflow._version import __version__
from jobflow.core.flow import Flow, JobOrder
from jobflow.core.job import Job, JobConfig, Response, job
from jobflow.core.maker import Maker
from jobflow.core.reference import OnMissing, OutputReference
from jobflow.core.state import CURRENT_JOB
from jobflow.core.store import JobStore
from jobflow.managers.local import run_locally

try:
    from jobflow.managers.prefect import PrefectManager, run_on_prefect
except ImportError:
    PrefectManager = None
    run_on_prefect = None
from jobflow.settings import JobflowSettings
from jobflow.utils.log import initialize_logger

SETTINGS = JobflowSettings()
