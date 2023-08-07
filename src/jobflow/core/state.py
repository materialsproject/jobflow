"""
Stateful interface for accessing the current job (and store).

This module defines the ``CURRENT_JOB`` object which has two attributes:

- ``job``: Containing the current job.
- ``store``: Containing the current store. Only available if ``expose_store`` is set in
  the job config.
"""

from __future__ import annotations

import typing

from monty.design_patterns import singleton

if typing.TYPE_CHECKING:
    pass

    import jobflow

__all__ = ["CURRENT_JOB"]


@singleton
class State:
    """State of the current job and store."""

    job: jobflow.Job = None
    store: jobflow.JobStore = None

    def reset(self):
        """Reset the current state."""
        self.job = None
        self.store = None


CURRENT_JOB: State = State()
