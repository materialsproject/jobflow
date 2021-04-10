from __future__ import annotations

import typing

from monty.design_patterns import singleton

if typing.TYPE_CHECKING:
    from typing import Optional

    import activities

__all__ = ["CURRENT_JOB"]


@singleton
class State:
    job: Optional[activities.Job] = None
    store: Optional[activities.ActivityStore] = None

    def reset(self):
        self.job = None
        self.store = None


CURRENT_JOB = State()
