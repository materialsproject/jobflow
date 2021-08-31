"""Define classes for handling job outputs."""

from __future__ import annotations

import typing
from typing import Any, Dict, Optional, Sequence, Tuple, Type

from monty.json import MontyDecoder, MontyEncoder, MSONable, jsanitize

from jobflow.utils.enum import ValueEnum

if typing.TYPE_CHECKING:

    from pydantic import BaseModel

    import jobflow

__all__ = [
    "OnMissing",
    "OutputReference",
    "resolve_references",
    "find_and_resolve_references",
    "find_and_get_references",
]


class OnMissing(ValueEnum):
    """
    What to do when a reference cannot be resolved.

    - ``ERROR``: Throw an error.
    - ``NONE``: Replace the missing reference with ``None``.
    - ``PASS``: Pass the unresolved :obj:`OutputReference` object on.
    """

    ERROR = "error"
    NONE = "none"
    PASS = "pass"


class OutputReference(MSONable):
    """
    A reference to the output of a :obj:`Job`.

    The output may or may not exist yet. Accordingly, :obj:`OutputReference` objects
    are essentially pointers to future job outputs. Output references can be used as
    input to a :obj:`Job` or :obj:`Flow` and will be "resolved". This means, the value
    of the reference will be looked up in the job store and the value passed on to the
    job.

    Output references are also how jobflow is able to determine the dependencies between
    jobs, allowing for construction of the job graph and to determine the job execution
    order.

    Upon attribute access or indexing, an output reference will return another
    output reference with the attribute accesses stored. When resolving the reference,
    the attributes/indexing will be also be performed before the value is passed on to
    the job. See the examples for more details.

    Parameters
    ----------
    uuid
        The job uuid to which the output belongs.
    attributes :
        A tuple of attributes or indexes that have been performed on the
        output. Attributes are specified by a tuple of ``("a", attr)``, whereas
        indexes are specified as a tuple of ``("i", index)``.
    output_schema
        A pydantic model that defines the schema of the output and that will be used to
        validate any attribute accesses or indexes. Note, schemas can only be used to
        validate the first attribute/index access.

    Examples
    --------
    A reference can be constructed using just the job uuid.

    >>> from jobflow import OutputReference
    >>> ref = OutputReference("1234")

    Attribute accesses return new references.

    >>> ref.my_attribute
    OutputReference(1234, .my_attribute)

    Attribute accesses and indexing can be chained.

    >>> ref["key"][0].value
    OutputReference(1234, ['key'], [0], .value)
    """

    __slots__ = ("uuid", "attributes", "output_schema")

    def __init__(
        self,
        uuid: str,
        attributes: Tuple[Tuple[str, Any], ...] = tuple(),
        output_schema: Optional[Type[BaseModel]] = None,
    ):
        super().__init__()
        self.uuid = uuid
        self.attributes = attributes
        self.output_schema = output_schema

        for attr_type, attr in attributes:
            if attr_type not in ("a", "i"):
                raise ValueError(
                    f"Unrecognised attribute type '{attr_type}' for attribute '{attr}'"
                )

    def resolve(
        self,
        store: Optional[jobflow.JobStore],
        cache: Optional[Dict[str, Any]] = None,
        on_missing: OnMissing = OnMissing.ERROR,
    ) -> Any:
        """
        Resolve the reference.

        This function will query the job store for the reference value and perform any
        attribute accesses/indexing.

        .. Note::
            When resolving multiple references simultaneously it is more efficient to
            use :obj:`.resolve_references` as it will minimize the number of database
            requests.

        Parameters
        ----------
        store
            A job store.
        cache
            A dictionary cache to use for local caching of reference values.
        on_missing
            What to do if the output reference is missing in the database and cache.
            See :obj:`OnMissing` for the available options.

        Raises
        ------
        ValueError
            If the reference cannot be found and ``on_missing`` is set to
            ``OnMissing.ERROR`` (default).

        Returns
        -------
        Any
            The resolved reference if it can be found. If the reference cannot be found,
            the returned value will depend on the value of ``on_missing``.
        """
        if cache is None:
            cache = {}

        if self.uuid not in cache:
            cache[self.uuid] = {}

        # get the latest index for the output
        result = store.query_one({"uuid": self.uuid}, ["index"], sort={"index": -1})
        index = None if result is None else result["index"]

        if index is not None and index not in cache[self.uuid]:
            try:
                cache[self.uuid][index] = store.get_output(
                    self.uuid, which="last", load=True, on_missing=on_missing
                )
            except ValueError:
                pass

        if on_missing == OnMissing.ERROR and index not in cache[self.uuid]:
            istr = f" ({index})" if index is not None else ""
            raise ValueError(
                f"Could not resolve reference - {self.uuid}{istr} not in store or cache"
            )
        elif on_missing == OnMissing.NONE and index not in cache[self.uuid]:
            return None
        elif on_missing == OnMissing.PASS and index not in cache[self.uuid]:
            return self

        data = cache[self.uuid][index]

        # decode objects before attribute access
        data = MontyDecoder().process_decoded(data)

        # re-cache data in case other references need it
        cache[self.uuid][index] = data

        for attr_type, attr in self.attributes:
            if attr_type == "i":
                # index
                data = data[attr]
            else:
                # attribute access
                data = getattr(data, attr)

        return data

    def set_uuid(self, uuid: str, inplace=True) -> OutputReference:
        """
        Set the UUID of the reference.

        Parameters
        ----------
        uuid
            A new UUID.
        inplace
            Whether to update the current reference object or return a completely new
            object.

        Returns
        -------
        OutputReference
            An output reference with the specified uuid.
        """
        if inplace:
            self.uuid = uuid
            return self
        else:
            from copy import deepcopy

            new_reference = deepcopy(self)
            new_reference.uuid = uuid
            return new_reference

    def __getitem__(self, item) -> OutputReference:
        """Index the reference."""
        if self.output_schema is not None:
            validate_schema_access(self.output_schema, item)

        return OutputReference(self.uuid, self.attributes + (("i", item),))

    def __getattr__(self, item) -> OutputReference:
        """Attribute access of the reference."""
        if item in {"kwargs", "args", "schema"} or (
            isinstance(item, str) and item.startswith("__")
        ):
            # This is necessary to trick monty/pydantic.
            raise AttributeError

        if self.output_schema is not None:
            validate_schema_access(self.output_schema, item)

        return OutputReference(self.uuid, self.attributes + (("a", item),))

    def __setattr__(self, attr, val):
        """Set attribute."""
        # Setting unknown attributes is not allowed
        if attr in self.__slots__:
            object.__setattr__(self, attr, val)
        else:
            raise TypeError("OutputReference objects are immutable")

    def __setitem__(self, index, val):
        """Set item."""
        # Setting items via indexing is not allowed.
        raise TypeError("OutputReference objects are immutable")

    def __repr__(self) -> str:
        """Get a string representation of the reference and attributes."""
        if len(self.attributes) > 0:
            attribute_str = ", " + ", ".join(self.attributes_formatted)
        else:
            attribute_str = ""

        return f"OutputReference({str(self.uuid)}{attribute_str})"

    def __hash__(self) -> int:
        """Return a hash of the reference."""
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        """Test for equality against another reference."""
        if isinstance(other, OutputReference):
            return (
                self.uuid == other.uuid
                and len(self.attributes) == len(other.attributes)
                and all(
                    [
                        a[0] == b[0] and a[1] == b[1]
                        for a, b in zip(self.attributes, other.attributes)
                    ]
                )
            )
        return False

    @property
    def attributes_formatted(self):
        """Get a formatted description of the attributes."""
        return [
            f".{x[1]}" if x[0] == "a" else f"[{repr(x[1])}]" for x in self.attributes
        ]

    def as_dict(self):
        """Serialize the reference as a dict."""
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
    store: jobflow.JobStore,
    cache: Optional[Dict[str, Any]] = None,
    on_missing: OnMissing = OnMissing.ERROR,
) -> Dict[OutputReference, Any]:
    """
    Resolve multiple output references.

    Uses caching to minimize number of database queries.

    Parameters
    ----------
    references :
        A list or tuple of output references.
    store
        A job store.
    cache
        A dictionary cache to use for local caching of reference values.
    on_missing
        What to do if the output reference is missing in the database and cache.
        See :obj:`OnMissing` for the available options.

    Returns
    -------
    dict[OutputReference, Any]
        The output values as a dictionary mapping of ``{reference: output}``.
    """
    from itertools import groupby

    resolved_references = {}
    if cache is None:
        cache = {}

    for uuid, ref_group in groupby(references, key=lambda x: x.uuid):
        # get latest index
        result = store.query_one({"uuid": uuid}, ["index"], sort={"index": -1})
        index = None if result is None else result["index"]

        if uuid not in cache:
            cache[uuid] = {}

        if index is not None and index not in cache[uuid]:
            cache[uuid][index] = store.get_output(
                uuid, load=True, on_missing=on_missing
            )

        for ref in ref_group:
            resolved_references[ref] = ref.resolve(
                store, cache=cache, on_missing=on_missing
            )

    return resolved_references


def find_and_get_references(arg: Any) -> Tuple[OutputReference, ...]:
    """
    Find and extract output references.

    This function works on nested inputs. For example, lists or dictionaries
    (or combinations of list and dictionaries) that contain output references.

    Parameters
    ----------
    arg
        The argument to search for references.

    Returns
    -------
    tuple[OutputReference]
        The output references as a tuple.
    """
    from pydash import get

    from jobflow.utils.find import find_key_value

    if isinstance(arg, OutputReference):
        # if the argument is a reference then stop there
        return tuple([arg])

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find a reference here
        return tuple()

    arg = jsanitize(arg, strict=True, enum_values=True)

    # recursively find any reference classes
    locations = find_key_value(arg, "@class", "OutputReference")

    # deserialize references and return
    return tuple([OutputReference.from_dict(get(arg, loc)) for loc in locations])


def find_and_resolve_references(
    arg: Any,
    store: jobflow.JobStore,
    cache: Optional[Dict[str, Any]] = None,
    on_missing: OnMissing = OnMissing.ERROR,
) -> Any:
    """
    Return the input but with all output references replaced with their resolved values.

    This function works on nested inputs. For example, lists or dictionaries
    (or combinations of list and dictionaries) that contain output references.

    Parameters
    ----------
    arg
        The input argument containing output references.
    store
        A job store.
    cache
        A dictionary cache to use for local caching of reference values.
    on_missing
        What to do if the output reference is missing in the database and cache.
        See :obj:`OnMissing` for the available options.

    Returns
    -------
    Any
        The input argument but with all output references replaced with their resolved
        values. If a reference cannot be found, its replacement value will depend on the
        value of ``on_missing``.
    """
    from pydash import get, set_

    from jobflow.utils.find import find_key_value

    if isinstance(arg, dict) and arg.get("@class") == "OutputReference":
        # if arg is a deserialized reference, serialize it
        arg = OutputReference.from_dict(arg)

    if isinstance(arg, OutputReference):
        # if the argument is a reference then stop there
        return arg.resolve(store, cache=cache, on_missing=on_missing)

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find a reference here
        return arg

    # serialize the argument to a dictionary
    encoded_arg = jsanitize(arg, strict=True, enum_values=True)

    # recursively find any reference classes
    locations = find_key_value(encoded_arg, "@class", "OutputReference")

    if len(locations) == 0:
        return arg

    # resolve the references
    references = [
        OutputReference.from_dict(get(encoded_arg, list(loc))) for loc in locations
    ]
    resolved_references = resolve_references(
        references, store, cache=cache, on_missing=on_missing
    )

    # replace the references in the arg dict
    for location, reference in zip(locations, references):
        # skip references that have not been resolved, e.g., on missing is PASS
        if reference == resolved_references[reference]:
            continue

        resolved_reference = resolved_references[reference]
        set_(encoded_arg, list(location), resolved_reference)

    # deserialize dict array
    return MontyDecoder().process_decoded(encoded_arg)


def validate_schema_access(schema: Type[BaseModel], item: str):
    """
    Validate that an attribute or index access is supported by a model.

    Parameters
    ----------
    schema
        A pydantic model to use as the schema.
    item
        An attribute or key to access.

    Raises
    ------
    AttributeError
        If the item is not a valid attribute of the schema.

    Returns
    -------
    bool
        Returns ``True`` if the schema access was valid.
    """
    schema_dict = schema.schema()
    if item not in schema_dict["properties"]:
        raise AttributeError(f"{schema.__name__} does not have attribute '{item}'.")
    return True
