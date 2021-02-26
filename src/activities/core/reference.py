from dataclasses import dataclass
from typing import Any, Optional, Tuple
from uuid import UUID

from monty.json import MSONable


@dataclass
class Reference(MSONable):

    uuid: UUID
    name: str
    attributes: Optional[Tuple[Any]] = tuple()

    def __getitem__(self, item) -> "Reference":
        return Reference(self.uuid, self.name, self.attributes + (item,))

    def __str__(self):
        if len(self.attributes) > 0:
            attribute_str = ", " + ", ".join(map(str, self.attributes))
        else:
            attribute_str = ""

        return f"Reference({str(self.uuid)}, {self.name}{attribute_str})"

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other: "Reference") -> bool:
        return (
            self.uuid == other.uuid
            and self.name == other.name
            and self.attributes == other.attributes
        )
