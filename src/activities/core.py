from abc import ABC, abstractmethod
from typing import Tuple, Dict
from uuid import UUID

from activities.reference import Reference


class HasInputOutput(ABC):

    @property
    @abstractmethod
    def input_references(self) -> Tuple[Reference, ...]:
        pass

    @property
    @abstractmethod
    def output_references(self) -> Tuple[Reference, ...]:
        pass

    @property
    def input_uuids(self) -> Tuple[UUID, ...]:
        return tuple([ref.uuid for ref in self.input_references])

    @property
    def input_references_grouped(self) -> Dict[UUID, Tuple[Reference, ...]]:
        from collections import defaultdict

        groups = defaultdict(set)
        for ref in self.input_references:
            groups[ref.uuid].add(ref)

        return {k: tuple(v) for k, v in groups.items()}

    @property
    def output_uuids(self) -> Tuple[UUID, ...]:
        return tuple([ref.uuid for ref in self.output_references])

    @property
    def output_references_grouped(self) -> Dict[UUID, Tuple[Reference, ...]]:
        from collections import defaultdict

        groups = defaultdict(set)
        for ref in self.output_references:
            groups[ref.uuid].add(ref)

        return {k: tuple(v) for k, v in groups.items()}
