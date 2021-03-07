"""This module defines the base Output classes."""
from __future__ import annotations

import logging
import typing
from abc import ABC
from dataclasses import dataclass
from uuid import uuid4

from monty.json import MSONable

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Generator, List, Optional, Tuple, Type, TypeVar
    from uuid import UUID

    from maggma.core import Store

    import activities

    T = TypeVar("T", bound="Outputs")

logger = logging.getLogger(__name__)

__all__ = ["Outputs", "Value", "Number", "String", "Boolean", "Bytes", "Dynamic"]


class Outputs(MSONable, ABC):
    """
    Abstract class representing the outputs of a :obj:`.Task` or :obj:`.Activity`.

    Outputs can be used to enable static parameter checking, so that the connections
    between tasks can be validated before an activity is submitted. It also allows
    for tasks to have default values and enforce a schema for the task output.

    Outputs are also used as the basis for tracking references between tasks and
    activities. For this reason, all :obj:`Outputs` classes contains helper methods
    for initializing their fields with :obj:`.Reference` objects, and for storing the
    outputs in a maggma store.

    This class should not be used directly but instead subclassed to customise the
    outputs for a specific task.

    Examples
    --------
    The easiest way to create a usable outputs class is using the
    :obj:`dataclasses.dataclass` decorator.

    >>> from dataclasses import dataclass
    ...
    ... @dataclass
    ... class MyOutputs(Outputs):
    ...     my_string: str
    ...
    ... outputs = MyOutputs("I am an Outputs")

    Using the dataclass approach, outputs can have multiple fields and optional values.

    >>> @dataclass
    ... class MyOutputs(Outputs):
    ...     my_string: str
    ...     my_int: int
    ...     optional_flag: bool = False
    ...
    ... outputs = MyOutputs(my_string="I am an Outputs", my_int=2, optional_flag=True)
    ... outputs.optional_flag
    True

    Much more information is provided in the Python :obj:`dataclasses` module
    documention.

    All outputs classes can be initialized with references for all fields.

    >>> output_reference = MyOutputs.with_references()
    ... output_reference.my_string
    Reference(4c79bd82-2400-4c25-bc12-6806ca595e53, "my_string")

    To avoid having to create new outputs classes for simple tasks, we have implemented
    several basic outputs objects.

    ==============  =========  ========================================================
    Name             Field                              Note
    ==============  =========  ========================================================
    :obj:`Value`    `value`    Output for any value (default if single output returned)
    :obj:`Number`   `number`   Output for numerical values.
    :obj:`String`   `string`   Output for string values.
    :obj:`Boolean`  `boolean`  Output for string values.
    :obj:`Bytes`    `bytes`    Output for byte values.
    ==============  =========  ========================================================

    For cases where you don't know the format of the data in advance or don't want to
    enforce a schema, the :obj:`Dynmic` output class can be used. Here, fields are
    added to the outputs dynamically when the attributes are accessed. This class is
    used by default for tasks where the outputs class is not specified. One downside
    is that dynamic outputs cannot be used for static parameter checking. I.e., it isn't
    possible to check whether output referneces between tasks are valid before the tasks
    are run.

    >>> from activities.core.outputs import Dynamic
    ...
    ... my_outputs = Dynamic()
    ... my_outputs.this_field_does_not_exist
    Reference(d4ec97f5-4712-4b77-9ba4-07af18e7273a, "this_field_does_not_exist")
    """

    @property
    def references(self) -> Tuple[activities.Reference, ...]:
        """
        Returns any :obj:`.Reference` objects in the outputs.

        Returns
        -------
        (Reference, ...)
            The references in outputs class.
        """
        from activities.core.reference import find_and_get_references

        references = []
        for name in self.fields:
            if hasattr(self, name):
                references.extend(find_and_get_references(getattr(self, name)))

        return tuple(references)

    def resolve(
        self: T,
        output_store: Optional[Store] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
        error_on_missing: bool = True,
    ) -> T:
        """
        Resolve (find and replace) all :obj:`.Reference` objects in the outputs.

        Parameters
        ----------
        output_store
            A maggma store to use for resolving references.
        output_cache
            A cache dictionary to use for resolving references.
        error_on_missing
            Whether to raise an error if a reference cannot be resolved.

        Returns
        -------
        Outputs
            An outputs with the references resolved.
        """
        from activities.core.reference import find_and_resolve_references

        resolved_outputs = find_and_resolve_references(
            self,
            output_store=output_store,
            output_cache=output_cache,
            error_on_missing=error_on_missing,
        )

        if isinstance(resolved_outputs, dict):
            # monty could not determine outputs class, probably because the class was
            # defined in a function
            resolved_outputs = self.from_dict(resolved_outputs)

        return resolved_outputs

    def to_store(self, output_store: Store, uuid: UUID):
        """
        Put the outputs in a store so they can be resolved later.

        Parameters
        ----------
        output_store
            A maggma store.
        uuid
            The unique identifier of the outputs (typically the activity uuid from
            which the outputs were generated.
        """
        if len(self.references) > 0:
            logger.warning(
                "Outputs contains references – call resolve() before to_store()"
            )

        # serialize to dict
        data = self.as_dict()
        data["uuid"] = str(uuid)
        output_store.update(data, key="uuid")

    def items(self) -> Generator[Tuple[str, Any], None, None]:
        """
        Generator for iterating over the outputs' fields and their values.

        Yields
        ------
        (str, Any)
            The output name and value.
        """
        for name in self.fields:
            yield name, getattr(self, name)

    @property
    def fields(self) -> Tuple[str]:
        """
        The fields (attributes) of the output object.

        Returns
        -------
        Tuple[str]
            A list of field names.
        """
        from inspect import signature

        sig = signature(self.__class__)
        return tuple(sig.parameters.keys())

    def with_references(self: T, uuid: Optional[UUID] = None) -> T:
        """
        Fill the outputs with :obj:`.Reference` objects for all fields.

        Parameters
        ----------
        uuid
            The unique identifier for the references.

        Returns
        -------
        cls
            The outputs class with references for all fields.
        """
        from activities import Reference

        if uuid is None:
            uuid = uuid4()

        references = {}
        for name in self.fields:
            references[name] = Reference(uuid, name)

        return self.__class__(**references)


class Dynamic(Outputs):
    """
    A special outputs class that creates its fields when they are referenced.

    The dynamic class can also be initialized with any number of keyword arguments.

    Parameters
    ----------
    kwargs
        The keyword arguments.
    """

    def __init__(self, *args, _uuid: UUID = None, **kwargs):
        self._uuid = _uuid if _uuid else uuid4()
        self._data = kwargs or {}

        if len(args) > 0:
            raise ValueError(
                "Dynamic output can only be initialized with keyword arguments"
            )

    @property
    def fields(self) -> Tuple[str]:
        """
        The fields (attributes) of the output object.

        Returns
        -------
        Tuple[str]
            A list of field names.
        """
        return tuple(self._data.keys())

    def __getitem__(self, item) -> Any:
        return getattr(self, item)

    def __getattr__(self, item) -> Any:
        if item in {"kwargs", "args"}:
            raise AttributeError

        if item in self._data:
            return self._data[item]

        from activities import Reference

        ref = Reference(self._uuid, item)
        self._data[item] = ref
        return ref

    def with_references(self: T, uuid: Optional[UUID] = None) -> T:
        """
        Fill the outputs with :obj:`.Reference` objects for all fields.

        Dynamic objects have no fields to begin with, as they are created when
        the attributes are accessed.

        Parameters
        ----------
        uuid
            The unique identifier for the references.

        Returns
        -------
        cls
            The outputs class with references for all fields.
        """
        from activities import Reference

        if uuid is None:
            uuid = uuid4()

        references = {}
        for name in self.fields:
            references[name] = Reference(uuid, name)

        return self.__class__(**references, _uuid=uuid)

    def resolve(
        self: T,
        output_store: Optional[Store] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
        error_on_missing: bool = True,
    ) -> T:
        """
        Resolve (find and replace) all :obj:`.Reference` objects in the outputs.

        Also finds all outputs in the store and cache with the same UUID as the output
        class. This behaviour is necessary as dynamic outputs may not be explicitly
        known when the class is created, and are instead determined by what outputs a
        task returns.

        Parameters
        ----------
        output_store
            A maggma store to use for resolving references.
        output_cache
            A cache dictionary to use for resolving references.
        error_on_missing
            Whether to raise an error if a reference cannot be resolved.

        Returns
        -------
        Outputs
            An outputs with the references resolved.
        """
        resolved_dict = super().resolve(
            output_store=output_store,
            output_cache=output_cache,
            error_on_missing=error_on_missing
        ).as_dict()
        if output_store:
            results = output_store.query_one({"uuid": str(self._uuid)})
            if results:
                resolved_dict.update(results)
        if output_cache and self._uuid in output_cache:
            resolved_dict.update(output_cache[self._uuid])
        return self.__class__(**resolved_dict)

    def as_dict(self):
        data = {
            "@module": self.__class__.__module__,
            "@class": self.__class__.__name__,
            "@version": None
        }
        data.update(self._data)
        return data

    def __repr__(self):
        fields = ", ".join([f"{k}={repr(v)}" for k, v in self._data.items()])
        return f"Dynamic({fields})"


@dataclass
class Value(Outputs):
    """
    An outputs class containing a value of any type.

    Parameters
    ----------
    value
        A value.
    """

    value: Any


@dataclass
class Bytes(Outputs):
    """
    An outputs class containing bytes.

    Parameters
    ----------
    bytes
        A bytes object.
    """

    bytes: bytes


@dataclass
class Number(Outputs):
    """
    An outputs class containing a numerical value.

    Parameters
    ----------
    number
        A number (float or int).
    """

    number: float


@dataclass
class String(Outputs):
    """
    An outputs class containing a string value.

    Parameters
    ----------
    string
        A string.
    """

    string: str


@dataclass
class Boolean(Outputs):
    """
    An outputs class containing a boolean value.

    Parameters
    ----------
    boolean
        A boolean.
    """

    boolean: bool
