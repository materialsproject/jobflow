from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union

from monty.json import MSONable

if typing.TYPE_CHECKING:

    import activities


@dataclass
class Maker(ABC, MSONable):
    name: str = "Maker"

    @abstractmethod
    def make(self, *args, **kwargs) -> Union[activities.Activity, activities.Job]:
        pass
