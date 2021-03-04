"""This module defines functions and classes for representing Task objects."""
import logging
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Hashable, Optional, Tuple, Type
from uuid import UUID, uuid4

from maggma.core import Store as MaggmaStore
from monty.json import MSONable

from activities.core.base import HasInputOutput
from activities.core.outputs import Outputs
from activities.core.reference import (
    Reference,
    find_and_get_references,
    find_and_resolve_references,
)

logger = logging.getLogger(__name__)

__all__ = ["task", "Task", "Detour", "Restart", "Store", "Stop", "TaskResponse"]


def task(method: Optional[Callable] = None, outputs: Optional[Type[Outputs]] = None):
    """
    Wraps a function to produce a ``Task``.

    Tasks are delayed function calls that can be used in an ``Activity``. A task is a
    composed of the function name, the arguments for the function, and the outputs
    of the function. This decorator makes it simple to create ``Task`` objects directly
    from a function definition. See the examples for more details.

    Parameters
    ----------
    method
        A function to wrap. This should not be specified directly and is implied
        by the decorator.
    outputs
        If the function returns an ``Outputs`` object, the ``outputs`` option should be
        specified to enable static parameter checking. If the function returns a
        ``Detour`` object, the then `outputs`` should be set ot the class of the
        detour activity outputs.

    Examples
    --------
    >>> @task
    ... def print_message():
    ...     print("I am a Task")
    ...
    ... print_task = print_message()
    ... type(print_task)
    <class 'activities.core.task.Task'>
    >>> print_task.function
    ('__main__', 'print_message')

    Tasks can have required and optional parameters.

    >>> @task
    ... def print_sum(a, b=0):
    ...     return print(a + b)
    ...
    ... print_sum_task = print_sum(1, 2)
    ... print_sum_task.args
    (1, )
    ... print_sum_task.kwargs
    {"b": 2}

    If the function returns an ``Outputs`` object, the outputs class should be specified
    in the task options.

    >>> from activities.core.outputs import Number
    ...
    ... @task(outputs=Number)
    ... def add(a, b):
    ...     return Number(a + b)
    ...
    ... add_task = add(1, 2)
    ... add_task.outputs
    Number(number=Reference(abeb6f48-9b34-4698-ab69-e4dc2127ebe9', 'number'))

    Tasks can return ``Detour`` objects that cause new activities to be added to the
    Activity graph. In this case, the outputs class of the Detour activity should be
    specified in the task ``outputs`` option.

    >>> from activities import Detour, Activity
    ...
    ... @task(outputs=Number)
    ... def detour_add(a, b):
    ...     add_task = add(a, b)
    ...     activity = Activity("My detour", [add_task], add_task.outputs)
    ...     return Detour(activity)

    See Also
    --------
    Task
    Activity
    Outputs
    """

    def decorator(func):
        from functools import wraps

        @wraps(func)
        def get_task(*args, **kwargs) -> Task:
            module = func.__module__
            name = func.__name__
            return Task(
                function=(module, name), outputs=outputs, args=args, kwargs=kwargs
            )

        return get_task

    # See if we're being called as @task or @task().
    if method is None:
        # We're called with parens.
        return decorator

    # We're called as @task without parens.
    return decorator(method)


@dataclass
class Task(HasInputOutput, MSONable):

    function: Tuple[str, str]
    args: Tuple[Any] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    outputs: Optional[Outputs] = None
    uuid: UUID = field(default_factory=uuid4)

    def __post_init__(self):
        import inspect

        # if outputs exists and hasn't already been initialized
        if self.outputs and inspect.isclass(self.outputs):
            self.outputs = self.outputs.to_reference(self.uuid)

    def __call__(self, *args, **kwargs):
        return Task(
            function=self.function,
            args=args,
            kwargs=kwargs,
            outputs=self.outputs.to_reference(self.uuid),
        )

    @property
    def input_references(self) -> Tuple[Reference, ...]:
        references = set()
        for arg in tuple(self.args) + tuple(self.kwargs.values()):
            # TODO: could do this during init and store the references and their
            #   locations instead. Similarly, could store the serialized args and
            #   kwargs too.
            references.update(find_and_get_references(arg))

        return tuple(references)

    @property
    def output_references(self) -> Tuple[Reference, ...]:
        if self.outputs is None:
            return tuple()
        return self.outputs.references

    def run(
        self,
        output_store: Optional[MaggmaStore] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
    ) -> "TaskResponse":
        from importlib import import_module

        logger.info(f"Starting task - {self.function[1]} ({self.uuid})")

        module = import_module(self.function[0])
        function = getattr(module, self.function[1], None)

        if function is None:
            raise ValueError(f"Could not import {function} from {module}")

        # strip the wrapper so we can call the actual function
        function = function.__wrapped__

        self.resolve_args(output_store=output_store, output_cache=output_cache)
        all_returned_data = function(*self.args, **self.kwargs)
        response = TaskResponse.from_task_returns(all_returned_data, type(self.outputs))

        logger.info(f"Finished task - {self.function[1]} ({self.uuid})")
        return response

    def resolve_args(
        self,
        output_store: Optional[MaggmaStore] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
        error_on_missing: bool = True,
        inplace: bool = True,
    ) -> "Task":
        from copy import deepcopy

        resolved_args = find_and_resolve_references(
            self.args,
            output_store=output_store,
            output_cache=output_cache,
            error_on_missing=error_on_missing,
        )
        resolved_kwargs = find_and_resolve_references(
            self.kwargs,
            output_store=output_store,
            output_cache=output_cache,
            error_on_missing=error_on_missing,
        )

        if inplace:
            self.args = tuple(resolved_args)
            self.kwargs = resolved_kwargs
            return self

        new_task = deepcopy(self)
        new_task.args = resolved_args
        new_task.kwargs = resolved_kwargs
        return new_task


@dataclass
class Store:
    data: Dict[Hashable, Any]


@dataclass
class Detour:

    activity: "Activity"


@dataclass
class Restart:

    activity: "Activity"


@dataclass
class Stop:
    stop_tasks: bool = False
    stop_children: bool = False
    stop_activities: bool = False


@dataclass
class TaskResponse:
    # TODO: Consider merging this with ActivityResponse

    outputs: Optional[Outputs] = None
    detour: Optional["Activity"] = None
    restart: Optional["Activity"] = None
    store: Optional[Dict[str, Any]] = None
    stop_tasks: bool = False
    stop_children: bool = False
    stop_activities: bool = False

    @classmethod
    def from_task_returns(
        cls,
        task_returns: Optional[Any],
        task_output_class: Optional[str],
    ) -> "TaskResponse":
        from collections import defaultdict

        if task_returns is None:
            return TaskResponse()
        elif not isinstance(task_returns, (float, tuple)):
            task_returns = [task_returns]

        to_parse = defaultdict(list)
        for returned_data in task_returns:
            if isinstance(returned_data, Outputs):
                to_parse[Outputs].append(returned_data)
            elif isinstance(returned_data, (Detour, Restart, Store, Stop)):
                to_parse[type(returned_data)].append(returned_data)
            else:
                raise ValueError(
                    f"Unrecognised return type: {type(returned_data)}. Must be one of: "
                    "Output, Detour, Restart, Store, Stop}"
                )

        if Outputs in to_parse and Detour in to_parse:
            warnings.warn(
                "Outputs cannot not be specified at the same time as Detour. The "
                "outputs of the Detour activity will be used instead."
            )
            to_parse.pop(Outputs)

        task_response_data = {}
        for return_type, data in to_parse.items():
            if len(data) > 1:
                raise ValueError(
                    f"Only one {return_type} object can be returned per task."
                )

            data = data[0]
            if return_type == Outputs and task_output_class is None:
                warnings.warn(
                    "Task returned outputs but none were expected. "
                    "Outputs will be ignored"
                )
            elif return_type == Outputs:
                if type(data) != task_output_class:
                    warnings.warn(
                        f"Output class returned by task {type(data)} does "
                        f"not match expected output class {task_output_class}."
                    )
                task_response_data["outputs"] = data

            elif return_type == Store:
                task_response_data["store"] = data.data
            elif return_type == Detour:
                task_response_data["detour"] = data.activity
            elif return_type == Restart:
                task_response_data["detour"] = data.activity
            elif return_type == Stop:
                task_response_data["stop_tasks"] = data.stop_tasks
                task_response_data["stop_children"] = data.stop_children
                task_response_data["stop_activities"] = data.stop_activities

        return cls(**task_response_data)
