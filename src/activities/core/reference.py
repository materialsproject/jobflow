from __future__ import annotations

import typing
from dataclasses import dataclass

from monty.json import MontyDecoder, MontyEncoder, MSONable, jsanitize

from activities.core.config import ReferenceFallback

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Optional, Sequence, Tuple, Type
    from uuid import UUID

    from maggma.core import Store
    from pydantic.main import BaseModel


@dataclass
class Reference(MSONable):

    uuid: UUID
    attributes: Optional[Tuple[Any]] = tuple()
    schema: Optional[Type[BaseModel]] = None

    def resolve(
        self,
        store: Optional[Store] = None,
        cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
        on_missing: ReferenceFallback = ReferenceFallback.ERROR,
    ):
        # when resolving multiple references simultaneously it is more efficient
        # to use resolve_references as it will minimize the number of database requests
        if store is None and cache is None and on_missing == ReferenceFallback.ERROR:
            raise ValueError("At least one of store and cache must be set.")

        if cache is None:
            cache = {}

        if store and self.uuid not in cache:
            output = store.query_one(
                {"uuid": str(self.uuid)}, ["output"], {"index": -1}
            )
            if output is not None:
                cache[self.uuid] = output["output"]

        if on_missing == ReferenceFallback.ERROR and self.uuid not in cache:
            raise ValueError(
                f"Could not resolve reference - {self.uuid} not in store or cache"
            )

        try:
            data = cache[self.uuid]
        except KeyError:
            # if we get to here, that means the reference cannot be resolved
            if on_missing == ReferenceFallback.NONE:
                return None
            else:
                # only other option is ReferenceFallback.PASS
                return self

        for attribute in self.attributes:
            data = getattr(data, attribute)

        return data

    def set_uuid(self, uuid: UUID, inplace=True):
        if inplace:
            self.uuid = uuid
            return self
        else:
            from copy import deepcopy

            new_reference = deepcopy(self)
            new_reference.uuid = uuid
            return new_reference

    def __getitem__(self, item) -> "Reference":
        if self.schema is not None:
            validate_schema_access(self.schema, item)

        return Reference(self.uuid, self.attributes + (item,))

    def __getattr__(self, item) -> "Reference":
        if item in {"kwargs", "args"} or (
            isinstance(item, str) and item.startswith("__")
        ):
            raise AttributeError

        if self.schema is not None:
            validate_schema_access(self.schema, item)

        return Reference(self.uuid, self.attributes + (item,))

    def __repr__(self):
        if len(self.attributes) > 0:
            attribute_str = ", " + ", ".join(map(repr, self.attributes))
        else:
            attribute_str = ""

        return f"Reference({str(self.uuid)}{attribute_str})"

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Reference):
            return (
                self.uuid == other.uuid
                and len(self.attributes) == len(other.attributes)
                and all([a == b for a, b in zip(self.attributes, other.attributes)])
            )
        return False

    def as_dict(self):
        data = {
            "@module": self.__class__.__module__,
            "@class": self.__class__.__name__,
            "@version": None,
            "uuid": MontyEncoder().default(self.uuid),
            "attributes": self.attributes,
        }
        return data


def resolve_references(
    references: Sequence[Reference],
    store: Store,
    on_missing: ReferenceFallback = ReferenceFallback.ERROR,
) -> Dict[Reference, Any]:
    from itertools import groupby

    resolved_references = {}
    cache = {}

    for uuid, ref_group in groupby(references, key=lambda x: x.uuid):
        output = store.query_one({"uuid": str(uuid)}, ["output"], {"index": -1})
        if output is not None:
            cache[uuid] = output["output"]

        for ref in ref_group:
            resolved_references[ref] = ref.resolve(cache=cache, on_missing=on_missing)

    return resolved_references


def find_and_get_references(arg: Any) -> Tuple[Reference, ...]:
    import json

    from pydash import get

    from activities.core.util import find_key_value

    if isinstance(arg, Reference):
        # if the argument is a reference then stop there
        return tuple([arg])

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find a reference here
        return tuple()

    arg = jsanitize(arg, strict=True)

    # recursively find any reference classes
    locations = find_key_value(arg, "@class", "Reference")

    # deserialize references and return
    return tuple([Reference.from_dict(get(arg, loc)) for loc in locations])


def find_and_resolve_references(
    arg: Any,
    store: Store,
    on_missing: ReferenceFallback = ReferenceFallback.ERROR,
) -> Any:
    from pydash import get, set_

    from activities.core.util import find_key_value

    if isinstance(arg, Reference):
        # if the argument is a reference then stop there
        return arg.resolve(store=store, on_missing=on_missing)

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find a reference here
        return arg

    # serialize the argument to a dictionary
    encoded_arg = jsanitize(arg, strict=True)

    # recursively find any reference classes
    locations = find_key_value(encoded_arg, "@class", "Reference")

    if len(locations) == 0:
        return arg

    # resolve the references
    references = [Reference.from_dict(get(encoded_arg, list(loc))) for loc in locations]
    resolved_references = resolve_references(
        references,
        store,
        on_missing=on_missing,
    )

    # replace the references in the arg dict
    for location, reference in zip(locations, references):
        resolved_reference = resolved_references[reference]
        set_(encoded_arg, list(location), resolved_reference)

    # deserialize dict array
    return MontyDecoder().process_decoded(encoded_arg)


def validate_schema_access(schema: Type[BaseModel], item: str):
    schema_dict = schema.schema()
    if item not in schema_dict["properties"]:
        raise AttributeError(f"{schema.__name__} does not have attribute '{item}'.")
    return True
