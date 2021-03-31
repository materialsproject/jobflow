from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import Optional
    from uuid import UUID
    from maggma.stores import Store

__all__ = ["CURRENT_JOB"]


class _State:
    uuid: Optional[UUID] = None
    store: Optional[Store] = None

    def reset(self):
        self.uuid = None
        self.store = None


CURRENT_JOB = _State()
