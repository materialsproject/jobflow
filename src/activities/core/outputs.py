from abc import ABC
from typing import Optional, Sequence, Tuple
from uuid import UUID, uuid4

from monty.json import MSONable

from activities.core.reference import Reference


class Outputs(ABC, MSONable):
    @property
    def references(self) -> Tuple[Reference, ...]:
        from inspect import signature

        sig = signature(self.__class__)

        references = []
        for name in sig.parameters.keys():
            if hasattr(self, name) and isinstance(getattr(self, name), Reference):
                references.append(getattr(self, name))
        return tuple(references)

    @property
    def references_uuids(self) -> Sequence[UUID]:
        return [ref.uuid for ref in self.references]

    @classmethod
    def reference(cls, uuid: Optional[UUID] = None) -> "Outputs":
        from inspect import signature

        sig = signature(cls)

        if uuid is None:
            uuid = uuid4()

        references = {}
        for name in sig.parameters.keys():
            references[name] = Reference(uuid, name)

        return cls(**references)
