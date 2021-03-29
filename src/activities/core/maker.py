from __future__ import annotations

from dataclasses import dataclass

import typing
from typing import Union

from abc import ABC, abstractmethod

from monty.json import MSONable

if typing.TYPE_CHECKING:

    import activities


@dataclass
class Maker(ABC, MSONable):
    name: str = "Maker"

    @abstractmethod
    def make(self, *args, **kwargs) -> Union[activities.Activity, activities.Task]:
        pass
