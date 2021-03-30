"""This module defines functions and classes for representing Job objects."""
from __future__ import annotations

import json
import logging
import typing
from collections import Iterable
from dataclasses import dataclass, field
from uuid import uuid4

from monty.json import MontyEncoder, MSONable

from activities.core.base import HasInputOutput
from activities.core.reference import Reference

if typing.TYPE_CHECKING:
    from typing import Any, Callable, Dict, Hashable, Optional, Tuple, Type, Union
    from uuid import UUID

    from maggma.core import Store
    from pydantic.main import BaseModel

    import activities

logger = logging.getLogger(__name__)

__all__ = ["job", "Job", "Response", "store_output"]


def job(
    method: Optional[Callable] = None,
    output_schema: Optional[Type[BaseModel]] = None,
    name: Optional[str] = None,
):
    """
    Wraps a function to produce a :obj:`Job`.

    :obj:`Job` objects are delayed function calls that can be used in an
    :obj:`Activity`. A job is a composed of the function name, the arguments for the
    function, and the outputs of the function. This decorator makes it simple to create
    job objects directly from a function definition. See the examples for more details.

    Parameters
    ----------
    method
        A function to wrap. This should not be specified directly and is implied
        by the decorator.
    outputs
        If the function returns an :obj:`Outputs` object, the outputs option should be
        specified to enable static parameter checking. If the function returns a
        :obj:`Detour` object, then the outputs should be set os the class of the
        detour activity outputs.

    Examples
    --------
    >>> @job
    ... def print_message():
    ...     print("I am a Job")
    >>> print_job = print_message()
    >>> type(print_job)
    <class 'activities.core.job.Job'>
    >>> print_job.function
    ('__main__', 'print_message')

    Jobs can have required and optional parameters.

    >>> @job
    ... def print_sum(a, b=0):
    ...     return print(a + b)
    ...
    >>> print_sum_job = print_sum(1, 2)
    >>> print_sum_job.args
    (1, )
    >>> print_sum_job.kwargs
    {"b": 2}

    If the function returns a value it can be referenced using the ``output``
    attribute of the job.

    >>> @job
    ... def add(a, b):
    ...     return a + b
    ...
    >>> add_task = add(1, 2)
    >>> add_task.output
    Reference('abeb6f48-9b34-4698-ab69-e4dc2127ebe9')

    .. Note::
        Because the task has not yet been run, the output value is :obj:`Reference`
        object. References are automatically converted to their computed values
        (resolved) when the task runs.

    If a dictionary of values is returned, the values can be referenced in the usual
    manner.

    >>> from activities.core.outputs import Number
    ...
    >>> @job
    ... def compute(a, b):
    ...     return {"sum": a + b, "product": a * b}
    ...
    >>> compute_task = compute(1, 2)
    >>> compute_task.output["sum"]
    Reference('abeb6f48-9b34-4698-ab69-e4dc2127ebe9', 'sum')

    A better approach is to use :obj:`Outputs` classes. These have several benefits
    including the ability to make use of static parameter checking to ensure that
    the task outputs are valid. To use an outputs class, it should be specified
    in the :obj:`task` decorator options.

    >>> from activities.core.outputs import Number
    ...
    >>> @job(outputs=Number)
    ... def add(a, b):
    ...     return Number(a + b)
    ...
    >>> add_task = add(1, 2)
    >>> add_task.outputs.number
    Number(number=Reference(abeb6f48-9b34-4698-ab69-e4dc2127ebe9', 'number'))
    >>> add_task.outputs.bad_output
    AttributeError: 'Number' object has no attribute 'bad_output'

    To indicate that a task has no outputs, the ``outputs`` parameter should be set to
    ``None``.
    >>> @job(outputs=None)
    ... def print_message(message):
    ...     print(message)

    Tasks can return :obj:`Detour` objects that cause new activities to be added to the
    Activity graph. In this case, the outputs class of the Detour activity should be
    specified in the task ``outputs`` option.

    >>> from activities import Activity
    >>> from activities.core.outputs import Number
    ...
    >>> @job(outputs=Number)
    ... def detour_add(a, b):
    ...     add_task = add(a, b)
    ...     activity = Activity("My detour", [add_task], add_task.outputs)
    ...     return Detour(activity)

    See Also
    --------
    Job, .Activity, .Outputs
    """

    def decorator(func):
        from functools import wraps

        @wraps(func)
        def get_task(*args, **kwargs) -> Job:
            func_module = func.__module__
            func_name = func.__name__
            return Job(
                function=(func_module, func_name),
                output_schema=output_schema,
                args=args,
                kwargs=kwargs,
                name=name,
            )

        return get_task

    # See if we're being called as @job or @job().
    if method is None:
        # We're called with parens.
        return decorator

    # We're called as @job without parens.
    return decorator(method)


@dataclass
class Job(HasInputOutput, MSONable):
    """
    A :obj:`Job` is a delayed function call that can be used in an :obj:`.Activity`.

    In general, one should not create :obj:`Job` objects directly but instead use
    the :obj:`job` decorator on a function. Any calls to a decorated function will
    return an :obj:`Job` object.

    Parameters
    ----------
    function
        The delayed function to run specified as a tuple of (module, function_name).
    args
        The positional arguments to the function call.
    kwargs
        The keyword arguments to the function call.
    output
        The output of the activity. Note that until the activity is run this will be
        a reference.
    output_schema
        A pydantic model that defines the schema of the output.
    uuid
        A unique identifier for the job.
    metadata
        A dictionary of information that will get stored alongside the

    Examples
    --------
    Builtin functions such as :obj:`print` can be specified using the ``builtins``
    module.

    >>> print_task = Job(function=("builtins", "print"), args=("I am a job", ))

    Other functions should specify the full module path.

    >>> Job(function=("os.path", "join"), args=("folder", "filename.txt"))

    To use custom functions in a job, the functions should be importable (i.e. not
    defined in another function). For example, if the following function is defined
    in the ``my_package`` module.

    >>> def add(a, b):
    ...     return a + b
    ...
    >>> add_job = Job(function=("my_package", "add"), args=(1, 2))

    :obj:`Job` objects can be executed using the :obj:`run()` method. The output is
    always a :obj:`Response` object that contains the outputs and other options that
    control the activity execution.

    >>> response = add_task.run()
    >>> response.outputs
    Value(value=3)

    The default output type of a job is a :obj:`Value` object that has a single
    field `value`. If the function returns more than one outputs then
    the output must be specified as dictionary or a custom :obj:`Outputs` class.

    Using an :obj:`.Outputs` object also enables static parameter checking.

    >>> from activities.core.outputs import Number
    ...
    >>> def add(a, b):
    ...     return Number(a + b)
    ...
    >>> add_task = Job(function=("my_package", "add"), args=(1, 2), outputs=Number)
    >>> response = add_task.run()
    >>> response.outputs
    Number(number=3)

    More details are given in the :obj:`job` decorator docstring.

    See Also
    --------
    job, Response, .Outputs
    """

    function: Tuple[str, str]
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    output_schema: Optional[Type[BaseModel]] = None
    uuid: UUID = field(default_factory=uuid4)
    name: Optional[str] = None
    output: Reference = field(init=False)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.output = Reference(self.uuid, schema=self.output_schema)
        if self.name is None:
            self.name = self.function[1]

    @property
    def input_references(self) -> Tuple[activities.Reference, ...]:
        """
        Find :obj:`.Reference` objects in the job inputs.

        Returns
        -------
        tuple(Reference, ...)
            The references in the inputs to the job.
        """
        from activities.core.reference import find_and_get_references

        references = set()
        for arg in tuple(self.args) + tuple(self.kwargs.values()):
            references.update(find_and_get_references(arg))

        return tuple(references)

    @property
    def output_references(self) -> Tuple[activities.Reference, ...]:
        """
        Find :obj:`.Reference` objects in the job outputs.

        Returns
        -------
        tuple(Reference, ...)
            The references belonging to the job outputs.
        """
        return tuple([self.output])

    @property
    def graph(self):
        from networkx import DiGraph

        edges = []
        for uuid, refs in self.input_references_grouped.items():
            properties = [
                ".".join(map(str, ref.attributes)) for ref in refs if ref.attributes
            ]
            properties = properties if len(properties) > 0 else ""
            edges.append((uuid, self.uuid, {"properties": properties}))

        graph = DiGraph()
        graph.add_node(self.uuid, object=self, type="job", label=self.name)
        graph.add_edges_from(edges)
        return graph

    def run(self, store: Optional[Store] = None) -> "Response":
        """
        Run the job.

        If the job has inputs that are :obj:`.Reference` objects, then they will need
        to be resolved before the job can run. See the docstring for
        :obj:`.Reference.resolve()` for more details.

        Parameters
        ----------
        store
            A maggma store to use for resolving references and storing job outputs.

        Returns
        -------
        Response
            A the response of the job, containing the outputs, and other settings
            that determine the activity execution.

        Raises
        ------
        ValueError
            If the job function cannot be imported.

        See Also
        --------
        Response, .Reference
        """
        from datetime import datetime
        from importlib import import_module

        logger.info(f"Starting job - {self.function[1]} ({self.uuid})")

        module = import_module(self.function[0])
        function = getattr(module, self.function[1], None)

        if function is None:
            raise ValueError(f"Could not import {function} from {module}")

        if hasattr(function, "__wrapped__"):
            # strip the wrapper so we can call the actual function
            function = function.__wrapped__

        self.resolve_args(store=store)

        response: Response = function(*self.args, **self.kwargs)
        if not isinstance(response, Response):
            response = Response.from_job_returns(response, self.output_schema)

        if response.detour is not None:
            # don't store job output on detours
            response.detour.set_uuid(self.uuid)
        elif store is not None:
            # serialize the output to a dictionary
            data = {
                "output": json.loads(MontyEncoder().encode(response.output)),
                "uuid": str(self.uuid),
                "completed_at": datetime.now().isoformat(),
            }
            data.update(self.metadata)
            store.update(data, key="uuid")

        logger.info(f"Finished job - {self.function[1]} ({self.uuid})")
        return response

    def set_uuid(self, uuid: UUID):
        self.uuid = uuid
        self.output = self.output.set_uuid(uuid)

    def resolve_args(
        self,
        store: Store,
        error_on_missing: bool = True,
        inplace: bool = True,
    ) -> "Job":
        """
        Resolve any :obj:`.Reference` objects in the input arguments.

        See the docstring for :obj:`.Reference.resolve()` for more details.

        Parameters
        ----------
        store
            A maggma store to use for resolving references.
        error_on_missing
            Whether to raise an error if a reference cannot be resolved.
        inplace
            Update the arguments of the current job or return a new job object.

        Returns
        -------
        Job
            A job with the references resolved.
        """
        from copy import deepcopy

        from activities.core.reference import find_and_resolve_references

        resolved_args = find_and_resolve_references(
            self.args,
            store=store,
            error_on_missing=error_on_missing,
        )
        resolved_kwargs = find_and_resolve_references(
            self.kwargs,
            store=store,
            error_on_missing=error_on_missing,
        )
        resolved_args = tuple(resolved_args)

        if inplace:
            self.args = resolved_args
            self.kwargs = resolved_kwargs
            return self

        new_job = deepcopy(self)
        new_job.args = resolved_args
        new_job.kwargs = resolved_kwargs
        return new_job


@dataclass
class Response:
    """
    The :obj:`Response` contains the output, detours, and stop commands of a job.

    Parameters
    ----------
    output
        The job output.
    detour
        An activity or job to detour to.
    restart
        An activity or job to replace the current job.
    store
        Data to be stored by the activity manager.
    stop_children
        Stop any children of the current activity.
    stop_activities
        Stop executing all remaining jobs.
    """

    output: Optional[Any] = None
    detour: Optional[Union[activities.Activity, Job]] = None
    restart: Optional[Union[activities.Activity, Job]] = None
    store: Optional[Dict[Hashable, Any]] = None
    stop_children: bool = False
    stop_activities: bool = False

    def __post_init__(self):
        if self.output is not None and self.detour is not None:
            raise ValueError(
                "output and detour cannot not be specified at the same time."
            )

    @classmethod
    def from_job_returns(
        cls,
        job_returns: Optional[Any],
        output_schema: Optional[Type[BaseModel]] = None,
    ) -> Response:
        """
        Generate a :obj:`Response` from the outputs of a :obj:`Job`.

        Parameters
        ----------
        job_returns
            The outputs of a job. Should be a single or list of :obj:`Outputs`,
            :obj:`Store`, :obj:`Detour`, :obj:`Restart`, or :obj:`Stop` objects. Only
            one of each type of object is supported.

            .. Warning::
                :obj:`Detour` and :obj:`Outputs` objects should not be specified
                simultaneously. The outputs of the detour activity will be used instead.

        output_schema
            The outputs class associated with the job. Used to enforce a schema for the
            outputs. Currently, only a warning will be given if the job outputs do not
            match the expected outputs class.

        Returns
        -------
        Response
            The job response controlling the data to store and activity execution
            options.

        Raises
        ------
        ValueError
            If the job returns type ares not :obj:`Outputs`, :obj:`Store`, :obj:`Detour`,
            :obj:`Restart`, or :obj:`Stop` objects.
        ValueError
            If more than one of the same return type is given.

        See Also
        --------
        .Outputs, Store, Detour, Restart, Stop
        """
        if isinstance(job_returns, Response):
            return job_returns

        if job_returns is None:
            return Response()

        if isinstance(job_returns, Iterable):
            # check that a Response object is not given as one of many outputs
            for r in job_returns:
                if isinstance(r, Response):
                    raise ValueError(
                        "Response cannot be returned in combination with other "
                        "outputs."
                    )

        if output_schema is not None:
            job_returns = output_schema(job_returns)

        return cls(output=job_returns)


@job
def store_output(outputs: Any):
    return outputs
