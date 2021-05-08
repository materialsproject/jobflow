from __future__ import annotations

import typing
from typing import Any, Dict, Optional, Sequence, Tuple, Type

from monty.json import MontyDecoder, MontyEncoder, MSONable, jsanitize
from pydantic import BaseModel

from jobflow.utils.enum import ValueEnum

if typing.TYPE_CHECKING:
    import jobflow


class OnMissing(ValueEnum):
    ERROR = "error"
    NONE = "none"
    PASS = "pass"


class OutputReference(MSONable):

    __slots__ = ("uuid", "attributes", "output_schema")

    def __init__(
        self,
        uuid: str,
        attributes: Tuple[Any, ...] = tuple(),
        output_schema: Optional[Any] = None,
    ):
        super().__init__()
        self.uuid = uuid
        self.attributes = attributes
        self.output_schema = output_schema

    def resolve(
        self,
        store: Optional[jobflow.JobStore] = None,
        cache: Optional[Dict[str, Dict[str, Any]]] = None,
        on_missing: OnMissing = OnMissing.ERROR,
    ):
        # when resolving multiple references simultaneously it is more efficient
        # to use resolve_references as it will minimize the number of database requests
        if store is None and cache is None and on_missing == OnMissing.ERROR:
            raise ValueError("At least one of store and cache must be set.")

        if cache is None:
            cache = {}

        if store and self.uuid not in cache:
            try:
                cache[self.uuid] = store.get_output(
                    self.uuid, which="latest", load=True
                )
            except ValueError:
                pass

        if on_missing == OnMissing.ERROR and self.uuid not in cache:
            raise ValueError(
                f"Could not resolve reference - {self.uuid} not in store or cache"
            )

        try:
            data = cache[self.uuid]
        except KeyError:
            # if we get to here, that means the reference cannot be resolved
            if on_missing == OnMissing.NONE:
                return None
            else:
                # only other option is OnMissing.PASS
                return self

        # resolve nested references
        data = find_and_resolve_references(
            data, store, cache=cache, on_missing=on_missing
        )

        # decode objects before attribute access
        data = MontyDecoder().process_decoded(data)

        # re-cache data in case other references need it
        cache[self.uuid] = data

        for attribute in self.attributes:
            try:
                data = data[attribute]
            except KeyError:
                data = getattr(data, attribute)

        return data

    def set_uuid(self, uuid: str, inplace=True):
        if inplace:
            self.uuid = uuid
            return self
        else:
            from copy import deepcopy

            new_reference = deepcopy(self)
            new_reference.uuid = uuid
            return new_reference

    def __getitem__(self, item) -> OutputReference:
        if self.output_schema is not None:
            validate_schema_access(self.output_schema, item)

        return OutputReference(self.uuid, self.attributes + (item,))

    def __getattr__(self, item) -> OutputReference:
        if item in {"kwargs", "args", "schema"} or (
            isinstance(item, str) and item.startswith("__")
        ):
            raise AttributeError

        if self.output_schema is not None:
            validate_schema_access(self.output_schema, item)

        return OutputReference(self.uuid, self.attributes + (item,))

    def __setattr__(self, attr, val):
        if attr in self.__slots__:
            object.__setattr__(self, attr, val)
        else:
            raise TypeError("OutputReference objects are immutable")

    def __setitem__(self, index, val):
        raise TypeError("OutputReference objects are immutable")

    def __repr__(self):
        if len(self.attributes) > 0:
            attribute_str = ", " + ", ".join(map(repr, self.attributes))
        else:
            attribute_str = ""

        return f"OutputReference({str(self.uuid)}{attribute_str})"

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, OutputReference):
            return (
                self.uuid == other.uuid
                and len(self.attributes) == len(other.attributes)
                and all([a == b for a, b in zip(self.attributes, other.attributes)])
            )
        return False

    def as_dict(self):
        schema = self.output_schema
        schema_dict = MontyEncoder().default(schema) if schema is not None else None
        data = {
            "@module": self.__class__.__module__,
            "@class": self.__class__.__name__,
            "@version": None,
            "uuid": self.uuid,
            "attributes": self.attributes,
            "output_schema": schema_dict,
        }
        return data


def resolve_references(
    references: Sequence[OutputReference],
    store: Optional[jobflow.JobStore],
    cache: Optional[Dict] = None,
    on_missing: OnMissing = OnMissing.ERROR,
) -> Dict[OutputReference, Any]:
    from itertools import groupby

    resolved_references = {}
    if cache is None:
        cache = {}

    for uuid, ref_group in groupby(references, key=lambda x: x.uuid):
        if uuid not in cache and store is not None:
            try:
                cache[uuid] = store.get_output(uuid, load=True)
            except ValueError:
                pass

        for ref in ref_group:
            resolved_references[ref] = ref.resolve(
                store=store, cache=cache, on_missing=on_missing
            )

    return resolved_references


def find_and_get_references(arg: Any) -> Tuple[OutputReference, ...]:
    from pydash import get

    from jobflow.utils.find import find_key_value

    if isinstance(arg, OutputReference):
        # if the argument is a reference then stop there
        return tuple([arg])

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find a reference here
        return tuple()

    arg = jsanitize(arg, strict=True)

    # recursively find any reference classes
    locations = find_key_value(arg, "@class", "OutputReference")

    # deserialize references and return
    return tuple([OutputReference.from_dict(get(arg, loc)) for loc in locations])


def find_and_resolve_references(
    arg: Any,
    store: Optional[jobflow.JobStore],
    cache: Optional[Dict] = None,
    on_missing: OnMissing = OnMissing.ERROR,
) -> Any:
    from pydash import get, set_

    from jobflow.utils.find import find_key_value

    if isinstance(arg, OutputReference):
        # if the argument is a reference then stop there
        return arg.resolve(store=store, cache=cache, on_missing=on_missing)

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find a reference here
        return arg

    # serialize the argument to a dictionary
    encoded_arg = jsanitize(arg, strict=True)

    # recursively find any reference classes
    locations = find_key_value(encoded_arg, "@class", "OutputReference")

    if len(locations) == 0:
        return arg

    # resolve the references
    references = [
        OutputReference.from_dict(get(encoded_arg, list(loc))) for loc in locations
    ]
    resolved_references = resolve_references(
        references,
        store,
        cache=cache,
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
