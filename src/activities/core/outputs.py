import warnings
from abc import ABC
from typing import Any, Dict, Optional, Sequence, Tuple
from uuid import UUID, uuid4

from maggma.core import Store
from monty.json import MSONable

from activities.core.reference import Reference, resolve_references


class Outputs(ABC, MSONable):
    def resolve(
        self,
        output_store: Optional[Store] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
    ) -> "Outputs":
        # note this function can add activity outputs to the output_cache
        from copy import deepcopy

        output_cache = output_cache or {}
        resolved_references = resolve_references(
            self.references, output_store=output_store, output_cache=output_cache
        )

        resolved_outputs = deepcopy(self)
        for name, reference in self.field_references:
            setattr(resolved_outputs, name, resolved_references[reference])

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
            if hasattr(self, name) and isinstance(getattr(self, name), Reference):
                references.append(getattr(self, name))
        return tuple(references)

    @property
    def field_references(self) -> Tuple[Tuple[str, Reference], ...]:
        # returns a tuple of the field name and the associated reference
        references = []
        for name in self.fields():
            if hasattr(self, name) and isinstance(getattr(self, name), Reference):
                references.append((name, getattr(self, name)))
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

    @property
    def field_references_grouped(self) -> Dict[UUID, Tuple[Tuple[str, Reference]]]:
        from collections import defaultdict

        groups = defaultdict(set)
        for name, ref in self.field_references:
            groups[ref.uuid].add((name, ref))

        return {k: tuple(v) for k, v in groups.items()}

    @classmethod
    def to_reference(cls, uuid: Optional[UUID] = None) -> "Outputs":
        if uuid is None:
            uuid = uuid4()

        references = {}
        for name in cls.fields():
            references[name] = Reference(uuid, name)

        return cls(**references)
