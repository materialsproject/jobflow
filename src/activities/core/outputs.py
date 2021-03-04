import warnings
from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple
from uuid import UUID, uuid4

from maggma.core import Store
from monty.json import MSONable

from activities.core.reference import (
    Reference,
    find_and_get_references,
    find_and_resolve_references,
    resolve_references,
)


class Outputs(ABC, MSONable):
    def resolve(
        self,
        output_store: Optional[Store] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
        error_on_missing: bool = True,
    ) -> "Outputs":
        # note this function can add activity outputs to the output_cache
        from copy import deepcopy

        resolved_outputs = find_and_resolve_references(
            deepcopy(self),
            output_store=output_store,
            output_cache=output_cache,
            error_on_missing=error_on_missing,
        )
        return resolved_outputs

    def to_db(self, output_store: Store, uuid: UUID):
        if len(self.references) > 0:
            warnings.warn("Outputs contains references – call resolve() before to_db()")

        # serialize to dict
        data = self.as_dict()
        data["uuid"] = str(uuid)
        output_store.update(data, key="uuid")

    @classmethod
    def fields(cls):
        from inspect import signature

        sig = signature(cls)
        return list(sig.parameters.keys())

    def items(self):
        for name in self.fields():
            if hasattr(self, name):
                yield name, getattr(self, name)

    @property
    def references(self) -> Tuple[Reference, ...]:
        references = []
        for name in self.fields():
            if hasattr(self, name):
                references.extend(find_and_get_references(getattr(self, name)))

        return tuple(references)

    @property
    def references_uuids(self) -> Sequence[UUID]:
        return [ref.uuid for ref in self.references]

    @property
    def references_grouped(self) -> Dict[UUID, Tuple[Reference]]:
        from collections import defaultdict

        groups = defaultdict(set)
        for ref in self.references:
            groups[ref.uuid].add(ref)

        return {k: tuple(v) for k, v in groups.items()}

    @classmethod
    def to_reference(cls, uuid: Optional[UUID] = None) -> "Outputs":
        if uuid is None:
            uuid = uuid4()

        references = {}
        for name in cls.fields():
            references[name] = Reference(uuid, name)

        return cls(**references)


@dataclass
class Number(Outputs):
    number: float


@dataclass
class String(Outputs):
    string: str


@dataclass
class Boolean(Outputs):
    boolean: bool


#
# class OutputSet(Outputs):
#
#     def __init__(self, ):
#
#
