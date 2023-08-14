"""Functions and classes for representing Job objects."""

from __future__ import annotations

import logging
import typing
import warnings
from dataclasses import dataclass, field

from monty.json import MSONable, jsanitize

from jobflow.core.reference import OnMissing, OutputReference
from jobflow.utils.uuid import suuid

if typing.TYPE_CHECKING:
    from typing import Any, Callable, Hashable, Sequence

    from networkx import DiGraph
    from pydantic import BaseModel

    import jobflow

logger = logging.getLogger(__name__)

__all__ = ["job", "Job", "Response", "JobConfig", "store_inputs"]


@dataclass
class JobConfig(MSONable):
    """
    The configuration parameters for a job.

    Parameters
    ----------
    resolve_references
        Whether to resolve any references before the job function is executed.
        If ``False`` the unresolved reference objects will be passed into the function
        call.
    on_missing_references
        What to do if the references cannot be resolved. The default is to throw an
        error.
    manager_config
        The configuration settings to control the manager execution.
    expose_store
        Whether to expose the store in :obj:`.CURRENT_JOB`` when the job is running.
    pass_manager_config
        Whether to pass the manager configuration on to detour, addition, and
        replacement jobs.
    response_manager_config
        The custom configuration to pass to a detour, addition, or replacement job.
        Using this kwarg will automatically take precedence over the behavior of
        ``pass_manager_config`` such that a different configuration than
        ``manger_config`` can be passed to downstream jobs.

    Returns
    -------
    JobConfig
        A :obj:`JobConfig` object.
    """

    resolve_references: bool = True
    on_missing_references: OnMissing = OnMissing.ERROR
    manager_config: dict = field(default_factory=dict)
    expose_store: bool = False
    pass_manager_config: bool = True
    response_manager_config: dict = field(default_factory=dict)


def job(method: Callable = None, **job_kwargs):
    """
    Wrap a function to produce a :obj:`Job`.

    :obj:`Job` objects are delayed function calls that can be used in an
    :obj:`Flow`. A job is composed of the function name and source and any
    arguments for the function. This decorator makes it simple to create
    job objects directly from a function definition. See the examples for more details.

    Parameters
    ----------
    method
        A function to wrap. This should not be specified directly and is implied
        by the decorator.
    **job_kwargs
        Other keyword arguments that will get passed to the :obj:`Job` init method.

    Examples
    --------
    >>> @job
    ... def print_message():
    ...     print("I am a Job")
    >>> print_job = print_message()
    >>> type(print_job)
    <class 'jobflow.core.job.Job'>
    >>> print_job.function
    <function print_message at 0x7ff72bdf6af0>

    Jobs can have required and optional parameters.

    >>> @job
    ... def print_sum(a, b=0):
    ...     return print(a + b)
    >>> print_sum_job = print_sum(1, 2)
    >>> print_sum_job.function_args
    (1, )
    >>> print_sum_job.function_kwargs
    {"b": 2}

    If the function returns a value it can be referenced using the ``output``
    attribute of the job.

    >>> @job
    ... def add(a, b):
    ...     return a + b
    >>> add_task = add(1, 2)
    >>> add_task.output
    OutputReference('abeb6f48-9b34-4698-ab69-e4dc2127ebe9')

    .. Note::
        Because the task has not yet been run, the output value is an
        :obj:`OutputReference` object. References are automatically converted to their
        computed values (resolved) when the task runs.

    If a dictionary of values is returned, the values can be indexed in the usual
    way.

    >>> @job
    ... def compute(a, b):
    ...     return {"sum": a + b, "product": a * b}
    >>> compute_task = compute(1, 2)
    >>> compute_task.output["sum"]
    OutputReference('abeb6f48-9b34-4698-ab69-e4dc2127ebe9', 'sum')

    .. Warning::
        If an output is indexed incorrectly, for example by trying to access a key that
        doesn't exist, this error will only be raised when the Job is executed.

    Jobs can return :obj:`.Response` objects that control the flow execution flow.
    For example, to replace the current job with another job, ``replace`` can be used.

    >>> from jobflow import Response
    >>> @job
    ... def replace(a, b):
    ...     new_job = compute(a, b)
    ...     return Response(replace=new_job)

    By default, job outputs are stored in the :obj`.JobStore` ``docs_store``. However,
    the :obj:`.JobStore` `additional_stores`` can also be used for job outputs. The
    stores are specified as keyword arguments, where the argument name gives the store
    name and the argument value is the type of data/key to store in that store. More
    details on the accepted key types are given in the :obj:`Job` docstring. In the
    example below, the "graph" key is stored in an additional store named "graphs" and
    the "data" key is stored in an additional store called "large_data".

    >>> @job(large_data="data", graphs="graph")
    ... def compute(a, b):
    ...     return {"data": b, "graph": a }

    .. Note::
        Using additional stores requires the :obj:`.JobStore` to be configured with
        the required store names present. See the :obj:`.JobStore` docstring for more
        details.

    See Also
    --------
    Job, .Flow, .Response
    """

    def decorator(func):
        from functools import wraps

        # unwrap staticmethod or classmethod decorators
        desc = next(
            (desc for desc in (staticmethod, classmethod) if isinstance(func, desc)),
            None,
        )

        if desc:
            func = func.__func__

        @wraps(func)
        def get_job(*args, **kwargs) -> Job:
            f = func
            if len(args) > 0:
                # see if the first argument has a function with the same name as
                # this function
                met = getattr(args[0], func.__name__, None)
                if met:
                    # if so, check to see if that function ha been wrapped and
                    # whether the unwrapped function is the same as this function
                    wrap = getattr(met, "__func__", None)
                    if getattr(wrap, "original", None) is func:
                        # Ah ha. The function is a bound method.
                        f = met
                        args = args[1:]

            return Job(
                function=f, function_args=args, function_kwargs=kwargs, **job_kwargs
            )

        get_job.original = func

        if desc:
            # rewrap staticmethod or classmethod decorators
            get_job = desc(get_job)

        return get_job

    # See if we're being called as @job or @job().
    if method is None:
        # We're called with parens.
        return decorator

    # We're called as @job without parens.
    return decorator(method)


class Job(MSONable):
    """
    A :obj:`Job` is a delayed function call that can be used in an :obj:`.Flow`.

    In general, one should not create :obj:`Job` objects directly but instead use
    the :obj:`job` decorator on a function. Any calls to a decorated function will
    return an :obj:`Job` object.

    Parameters
    ----------
    function
        A function. Can be a builtin function such as ``sum`` or any other function
        provided it can be imported. Class and static methods can also be used, provided
        the class is importable. Lastly, methods (functions bound to an instance of
        class) can be used, provided the class is :obj:`.MSONable`.
    function_args
        The positional arguments to the function call.
    function_kwargs
        The keyword arguments to the function call.
    output_schema
        A pydantic model that defines the schema of the output.
    uuid
        A unique identifier for the job. Generated automatically.
    index
        The index of the job (number of times the job has been replaced).
    name
        The name of the job. If not set it will be determined from ``function``.
    metadata
        A dictionary of information that will get stored alongside the job output.
    config
        The config setting for the job.
    hosts
        The list of UUIDs of the hosts containing the job. The object identified by one
        UUID of the list should be contained in objects identified by its subsequent
        elements.
    metadata_updates
        A list of updates for the metadata that will be applied to any Flow/Job
        generated by the job.
    config_updates
        A list of updates for the config that will be applied to any Flow/Job generated
        by the job.
    **kwargs
        Additional keyword arguments that can be used to specify which outputs to save
        in additional stores. The argument name gives the additional store name and the
        argument value gives the type of data to store in that additional store.
        The value can be ``True`` in which case all outputs are stored in the additional
        store, a dictionary key (string or enum), an :obj:`.MSONable` class, or a list
        of keys/classes.

    Attributes
    ----------
    output
        The output of the job. This is a reference to the future job output and
        can be used as the input to other Jobs or Flows.

    Returns
    -------
    Job
        A job.

    Examples
    --------
    Builtin functions such as :obj:`print` can be specified.

    >>> print_task = Job(function=print, args=("I am a job", ))

    Or other functions of the Python standard library.

    >>> import os
    >>> Job(function=os.path.join, args=("folder", "filename.txt"))

    To use custom functions, the functions should be importable (i.e. not
    defined in another function). For example, if the following function is defined
    in the ``my_package`` module.

    >>> def add(a, b):
    ...     return a + b
    >>> add_job = Job(function=add, args=(1, 2))

    More details are given in the :obj:`job` decorator docstring.

    See Also
    --------
    job, Response, .Flow
    """

    def __init__(
        self,
        function: Callable,
        function_args: tuple[Any, ...] = None,
        function_kwargs: dict[str, Any] = None,
        output_schema: type[BaseModel] = None,
        uuid: str = None,
        index: int = 1,
        name: str = None,
        metadata: dict[str, Any] = None,
        config: JobConfig = None,
        hosts: list[str] = None,
        metadata_updates: list[dict[str, Any]] = None,
        config_updates: list[dict[str, Any]] = None,
        **kwargs,
    ):
        from copy import deepcopy

        from jobflow.utils.find import contains_flow_or_job

        function_args = () if function_args is None else function_args
        function_kwargs = {} if function_kwargs is None else function_kwargs
        uuid = suuid() if uuid is None else uuid
        metadata = {} if metadata is None else metadata
        config = JobConfig() if config is None else config

        # make a deep copy of the function (means makers do not share the same instance)
        self.function = deepcopy(function)
        self.function_args = function_args
        self.function_kwargs = function_kwargs
        self.output_schema = output_schema
        self.uuid = uuid
        self.index = index
        self.name = name
        self.metadata = metadata
        self.config = config
        self.hosts = hosts or []
        self.metadata_updates = metadata_updates or []
        self.config_updates = config_updates or []
        self._kwargs = kwargs

        if sum(v is True for v in kwargs.values()) > 1:
            raise ValueError("Cannot select True for multiple additional stores.")

        if self.name is None:
            if self.maker is not None:
                self.name = self.maker.name
            else:
                self.name = getattr(function, "__qualname__", function.__name__)

        self.output = OutputReference(self.uuid, output_schema=self.output_schema)

        # check to see if job or flow is included in the job args
        # this is a possible situation but likely a mistake
        all_args = tuple(self.function_args) + tuple(self.function_kwargs.values())
        if contains_flow_or_job(all_args):
            warnings.warn(
                f"Job '{self.name}' contains an Flow or Job as an input. "
                f"Usually inputs should be the output of a Job or an Flow (e.g. "
                f"job.output). If this message is unexpected then double check the "
                f"inputs to your Job."
            )

    def __repr__(self):
        """Get a string representation of the job."""
        name, uuid = self.name, self.uuid
        return f"Job({name=}, {uuid=})"

    def __contains__(self, item: Hashable) -> bool:
        """
        Check if the job contains a reference to a given UUID.

        Parameters
        ----------
        item
            A UUID.

        Returns
        -------
        bool
            Whether the job contains a reference to the UUID.
        """
        return item in self.input_uuids

    def __eq__(self, other: object) -> bool:
        """
        Check if two jobs are equal.

        Parameters
        ----------
        other
            Another job.

        Returns
        -------
        bool
            Whether the jobs are equal.
        """
        if not isinstance(other, Job):
            return NotImplemented
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        """Get the hash of the job."""
        return hash(self.uuid)

    @property
    def input_references(self) -> tuple[jobflow.OutputReference, ...]:
        """
        Find :obj:`.OutputReference` objects in the job inputs.

        Returns
        -------
        tuple(OutputReference, ...)
            The references in the inputs to the job.
        """
        from jobflow.core.reference import find_and_get_references

        references: set[jobflow.OutputReference] = set()
        for arg in tuple(self.function_args) + tuple(self.function_kwargs.values()):
            references.update(find_and_get_references(arg))

        return tuple(references)

    @property
    def input_uuids(self) -> tuple[str, ...]:
        """
        Uuids of any :obj:`.OutputReference` objects in the job inputs.

        Returns
        -------
        tuple(str, ...)
           The UUIDs of the references in the job inputs.
        """
        return tuple(ref.uuid for ref in self.input_references)

    @property
    def input_references_grouped(self) -> dict[str, tuple[OutputReference, ...]]:
        """
        Group any :obj:`.OutputReference` objects in the job inputs by their UUIDs.

        Returns
        -------
        dict[str, tuple(OutputReference, ...)]
            The references grouped by their UUIDs.
        """
        from collections import defaultdict

        groups = defaultdict(set)
        for ref in self.input_references:
            groups[ref.uuid].add(ref)

        return {k: tuple(v) for k, v in groups.items()}

    @property
    def maker(self) -> jobflow.Maker | None:
        """
        Get the host :obj:`.Maker` object if a job is to run a maker function.

        Returns
        -------
        Maker or None
            A maker object.
        """
        from jobflow import Maker

        bound = getattr(self.function, "__self__", None)
        if isinstance(bound, Maker):
            return bound
        return None

    @property
    def graph(self) -> DiGraph:
        """
        Get a graph of the job indicating the inputs to the job.

        Returns
        -------
        DiGraph
            The graph showing the connectivity of the jobs.
        """
        from networkx import DiGraph

        edges = []
        for uuid, refs in self.input_references_grouped.items():
            properties: list[str] | str = [
                ref.attributes_formatted[-1]
                .replace("[", "")
                .replace("]", "")
                .replace(".", "")
                for ref in refs
                if ref.attributes
            ]
            properties = properties[0] if len(properties) == 1 else properties
            properties = properties if len(properties) > 0 else "output"
            edges.append((uuid, self.uuid, {"properties": properties}))

        graph = DiGraph()
        graph.add_node(self.uuid, job=self, label=self.name)
        graph.add_edges_from(edges)
        return graph

    @property
    def host(self):
        """
        UUID of the first Flow that contains the Job.

        Returns
        -------
        str
            the UUID of the host.
        """
        return self.hosts[0] if self.hosts else None

    def set_uuid(self, uuid: str) -> None:
        """
        Set the UUID of the job.

        Parameters
        ----------
        uuid
            A UUID.
        """
        self.uuid = uuid
        self.output = self.output.set_uuid(uuid)

    def run(self, store: jobflow.JobStore) -> Response:
        """
        Run the job.

        If the job has inputs that are :obj:`.OutputReference` objects, then they will
        need to be resolved before the job can run. See the docstring for
        :obj:`.OutputReference.resolve()` for more details.

        Parameters
        ----------
        store
            A :obj:`.JobStore` to use for resolving references and storing job outputs.

        Returns
        -------
        Response
            The response of the job, containing the outputs, and other settings that
            determine the flow execution.

        Raises
        ------
        ImportError
            If the job function cannot be imported.

        See Also
        --------
        Response, .OutputReference
        """
        import builtins
        import types
        from datetime import datetime

        from jobflow import CURRENT_JOB
        from jobflow.core.flow import get_flow

        index_str = f", {self.index}" if self.index != 1 else ""
        logger.info(f"Starting job - {self.name} ({self.uuid}{index_str})")
        CURRENT_JOB.job = self

        if self.config.expose_store:
            CURRENT_JOB.store = store

        if self.config.resolve_references:
            self.resolve_args(store=store)

        # if Job was created using the job decorator, then access the original function
        function = getattr(self.function, "original", self.function)

        # if function is bound method we need to do some magic to bind the unwrapped
        # function to the class/instance
        bound = getattr(self.function, "__self__", None)
        if bound is not None and bound is not builtins:
            function = types.MethodType(function, bound)

        response = function(*self.function_args, **self.function_kwargs)
        response = Response.from_job_returns(response, self.output_schema)

        if response.replace is not None:
            response.replace = prepare_replace(response.replace, self)

        if response.addition is not None:
            # wrap the detour in a Flow to avoid problems if it needs to get
            # wrapped at a later stage
            response.addition = get_flow(response.addition)

        if response.detour is not None:
            # wrap the detour in a Flow to avoid problems if it needs to get
            # wrapped at a later stage
            response.detour = get_flow(response.detour)

        # common actions that should be applied to all newly generated Jobs/Flows
        for new_jobs in (response.replace, response.addition, response.detour):
            if new_jobs is not None:
                new_jobs.add_hosts_uuids(self.hosts)
                for metadata_update in self.metadata_updates:
                    new_jobs.update_metadata(**metadata_update, dynamic=True)
                for config_update in self.config_updates:
                    new_jobs.update_config(**config_update, dynamic=True)

        if self.config.response_manager_config:
            passed_config = self.config.response_manager_config
        elif self.config.pass_manager_config:
            passed_config = self.config.manager_config
        else:
            passed_config = None

        if passed_config:
            if response.addition is not None:
                pass_manager_config(response.addition, passed_config)

            if response.detour is not None:
                pass_manager_config(response.detour, passed_config)

            if response.replace is not None:
                pass_manager_config(response.replace, passed_config)

        try:
            output = jsanitize(
                response.output, strict=True, enum_values=True, allow_bson=True
            )
        except AttributeError as err:
            raise RuntimeError(
                "Job output contained an object that is not MSONable and therefore "
                "could not be serialized."
            ) from err

        save = {k: "output" if v is True else v for k, v in self._kwargs.items()}
        data = {
            "uuid": self.uuid,
            "index": self.index,
            "output": output,
            "completed_at": datetime.now().isoformat(),
            "metadata": self.metadata,
            "hosts": self.hosts,
            "name": self.name,
        }
        store.update(data, key=["uuid", "index"], save=save)

        CURRENT_JOB.reset()
        logger.info(f"Finished job - {self.name} ({self.uuid}{index_str})")
        return response

    def resolve_args(
        self,
        store: jobflow.JobStore,
        inplace: bool = True,
    ) -> Job:
        """
        Resolve any :obj:`.OutputReference` objects in the input arguments.

        See the docstring for :obj:`.OutputReference.resolve()` for more details.

        Parameters
        ----------
        store
            A maggma store to use for resolving references.
        inplace
            Update the arguments of the current job or return a new job object.

        Returns
        -------
        Job
            A job with the references resolved.
        """
        from copy import deepcopy

        from jobflow.core.reference import find_and_resolve_references

        cache: dict[str, Any] = {}
        resolved_args = find_and_resolve_references(
            self.function_args,
            store,
            cache=cache,
            on_missing=self.config.on_missing_references,
        )
        resolved_kwargs = find_and_resolve_references(
            self.function_kwargs,
            store,
            cache=cache,
            on_missing=self.config.on_missing_references,
        )
        resolved_args = tuple(resolved_args)

        if inplace:
            self.function_args = resolved_args
            self.function_kwargs = resolved_kwargs
            return self

        new_job = deepcopy(self)
        new_job.function_args = resolved_args
        new_job.function_kwargs = resolved_kwargs
        return new_job

    def update_kwargs(
        self,
        update: dict[str, Any],
        name_filter: str = None,
        function_filter: Callable = None,
        dict_mod: bool = False,
    ):
        """
        Update the kwargs of the jobs.

        Parameters
        ----------
        update
            The updates to apply.
        name_filter
            A filter for the job name. Only jobs with a matching name will be updated.
            Includes partial matches, e.g. "ad" will match a job with the name "adder".
        function_filter
            A filter for the job function. Only jobs with a matching function will be
            updated.
        dict_mod
            Use the dict mod language to apply updates. See :obj:`.DictMods` for more
            details.

        Examples
        --------
        Consider a simple job with a ``number`` keyword argument.

        >>> from jobflow import job
        >>> @job
        ... def add(a, number=5):
        ...     return a + number
        >>> add_job = add(1)

        The ``number`` argument can be updated using.

        >>> add_job.update_kwargs({"number": 10})
        """
        from jobflow.utils.dict_mods import apply_mod

        if function_filter is not None and function_filter != self.function:
            return

        if (
            name_filter is not None
            and self.name is not None
            and name_filter not in self.name
        ):
            return

        # if we get to here then we pass all the filters
        if dict_mod:
            apply_mod(update, self.function_kwargs)
        else:
            self.function_kwargs.update(update)

    def update_maker_kwargs(
        self,
        update: dict[str, Any],
        name_filter: str = None,
        class_filter: type[jobflow.Maker] = None,
        nested: bool = True,
        dict_mod: bool = False,
    ):
        """
        Update the keyword arguments of any :obj:`.Maker` objects in the job source.

        Parameters
        ----------
        update
            The updates to apply.
        name_filter
            A filter for the Maker name. Only Makers with a matching name will be
            updated. Includes partial matches, e.g. "ad" will match a Maker with the
            name "adder".
        class_filter
            A filter for the maker class. Only Makers with a matching class will be
            updated. Note the class filter will match any subclasses.
        nested
            Whether to apply the updates to Maker objects that are themselves kwargs
            of Maker, job, or flow objects. See examples for more details.
        dict_mod
            Use the dict mod language to apply updates. See :obj:`.DictMods` for more
            details.

        Examples
        --------
        Consider the following job from a Maker:

        >>> from dataclasses import dataclass
        >>> from jobflow import job, Maker, Flow
        >>> @dataclass
        ... class AddMaker(Maker):
        ...     name: str = "add"
        ...     number: float = 10
        ...
        ...     @job
        ...     def make(self, a):
        ...         return a + self.number
        >>> maker = AddMaker()
        >>> add_job = maker.make(1)

        The ``number`` argument could be updated in the following ways.

        >>> add_job.update_maker_kwargs({"number": 10})

        By default, the updates are applied to nested Makers. These are Makers
        which are present in the kwargs of another Maker. Consider the following case
        for a Maker that produces a job that replaces itself with another job.

        >>> from jobflow import Response
        >>> @dataclass
        ... class ReplacementMaker(Maker):
        ...     name: str = "replace"
        ...     add_maker: Maker = AddMaker()
        ...
        ...     @job
        ...     def make(self, a):
        ...         add_job = self.add_maker.make(a)
        ...         return Response(replace=add_job)
        >>> maker = ReplacementMaker()
        >>> my_job = maker.make(1)

        The following update will apply to the nested ``AddMaker`` in the kwargs of the
        ``RestartMaker``:

        >>> my_job.update_maker_kwargs({"number": 10}, class_filter=AddMaker)

        However, if ``nested=False``, then the update will not be applied to the nested
        Maker:

        >>> my_job.update_maker_kwargs(
        ...     {"number": 10}, class_filter=AddMaker, nested=False
        ... )
        """
        from jobflow import Maker

        if self.maker is not None:
            maker = self.maker.update_kwargs(
                update,
                name_filter=name_filter,
                class_filter=class_filter,
                nested=nested,
                dict_mod=dict_mod,
            )
            self.function = getattr(maker, self.function.__name__)
        elif nested:
            # also look for makers in job args and kwargs
            new_args = []
            for arg in self.function_args:
                if isinstance(arg, Maker):
                    new_arg = arg.update_kwargs(
                        update,
                        name_filter=name_filter,
                        class_filter=class_filter,
                        nested=nested,
                        dict_mod=dict_mod,
                    )
                    new_args.append(new_arg)
                else:
                    new_args.append(arg)
            self.function_args = tuple(new_args)

            for name, kwarg in self.function_kwargs.items():
                if isinstance(kwarg, Maker):
                    self.function_kwargs[name] = kwarg.update_kwargs(
                        update,
                        name_filter=name_filter,
                        class_filter=class_filter,
                        nested=nested,
                        dict_mod=dict_mod,
                    )

    def append_name(self, append_str: str, prepend: bool = False):
        """
        Append a string to the name of the job.

        Parameters
        ----------
        append_str
            A string to append.
        prepend
            Prepend the name rather than appending it.
        """
        if prepend:
            self.name = append_str + self.name
        else:
            self.name += append_str

    def update_metadata(
        self,
        update: dict[str, Any],
        name_filter: str = None,
        function_filter: Callable = None,
        dict_mod: bool = False,
        dynamic: bool = True,
    ):
        """
        Update the metadata of the job.

        Can optionally apply the same updates at runtime to any Job or Flow generated
        by this job.

        Parameters
        ----------
        update
            The updates to apply.
        name_filter
            A filter for the job name. Only jobs with a matching name will be updated.
            Includes partial matches, e.g. "ad" will match a job with the name "adder".
        function_filter
            A filter for the job function. Only jobs with a matching function will be
            updated.
        dict_mod
            Use the dict mod language to apply updates. See :obj:`.DictMods` for more
            details.
        dynamic
            The updates will be propagated to Jobs/Flows dynamically generated at
            runtime.

        Examples
        --------
        Consider a simple job that makes use of a :obj:`Maker` to generate additional
        jobs at runtime (see :obj:`Response` options for more details):

        >>> @job
        ... def use_maker(maker):
        ...     return Response(replace=maker.make())

        Calling `update_metadata` with `dynamic` set to `True` (the default)

        >>> test_job = use_maker(ExampleMaker())
        ... test_job.update_metadata({"example": 1}, dynamic=True)

        will not only set the `example` metadata to the `test_job`, but also to all the
        new Jobs that will be generated at runtime by the ExampleMaker.

        `update_metadata` can be called multiple times with different `name_filter` or
        `function_filter` to control which Jobs will be updated.

        At variance, if `dynamic` is set to `False` the `example` metadata will only be
        added to the `test_job` and not to the generated Jobs.
        """
        from jobflow.utils.dict_mods import apply_mod

        if dynamic:
            dict_input = {
                "update": update,
                "name_filter": name_filter,
                "function_filter": function_filter,
                "dict_mod": dict_mod,
            }
            self.metadata_updates.append(dict_input)

        # unwrap the functions in case the job is a decorated one
        function_filter = getattr(function_filter, "__wrapped__", function_filter)
        function = getattr(self.function, "__wrapped__", self.function)

        # if function_filter is not None and function_filter != self.function:
        if function_filter is not None and function_filter != function:
            return

        if name_filter is not None and (
            self.name is None or name_filter not in self.name
        ):
            return

        # if we get to here then we pass all the filters
        if dict_mod:
            apply_mod(update, self.metadata)
        else:
            self.metadata.update(update)

    def update_config(
        self,
        config: JobConfig | dict,
        name_filter: str = None,
        function_filter: Callable = None,
        attributes: list[str] | str = None,
        dynamic: bool = True,
    ):
        """
        Update the job config.

        Can optionally apply the same updates at runtime to any Job or Flow generated
        by this job.

        Parameters
        ----------
        config
            A JobConfig object or a dict with containing the attributes to update.
        name_filter
            A filter for the job name. Only jobs with a matching name will be updated.
            Includes partial matches, e.g. "ad" will match a job with the name "adder".
        function_filter
            A filter for the job function. Only jobs with a matching function will be
            updated.
        attributes :
            Which attributes of the job config to set. Can be specified as one or more
            attributes specified by their name.
        dynamic
            The updates will be propagated to Jobs/Flows dynamically generated at
            runtime.

        Examples
        --------
        Consider a simple job.

        >>> from jobflow import job, JobConfig
        >>> @job
        ... def add(a, b):
        ...     return a + b
        >>> add_job = add(1, 2)

        The ``config`` can be updated using.

        >>> new_config = JobConfig(
        ...    manager_config={"_fworker": "myfworker"}, resolve_references=False
        ... )
        >>> add_job.update_config(new_config)

        To only update specific attributes, the ``attributes`` argument can be
        specified. For example, the following will only update the "manager_config"
        attribute of the job config.

        >>> add_job.update_config(new_config, attributes="manager_config")

        Alternatively, the config can be specified as a dictionary with keys that are
        attributes of the JobConfig object. This allows you to specify updates without
        having to create a completely new JobConfig object. For example:

        >>> add_job.update_config({"manager_config": {"_fworker": "myfworker"}})

        Consider instead a simple job that makes use of a :obj:`Maker` to generate
        additional jobs at runtime (see :obj:`Response` options for more details):

        >>> @job
        ... def use_maker(maker):
        ...     return Response(replace=maker.make())

        Calling `update_config` with `dynamic` set to `True` (the default)

        >>> test_job = use_maker(ExampleMaker())
        ... test_job.update_config({"manager_config": {"_fworker": "myfworker"}})

        will not only set the `manager_config` to the `test_job`, but also to all the
        new Jobs that will be generated at runtime by the ExampleMaker.

        `update_config` can be called multiple times with different `name_filter` or
        `function_filter` to control which Jobs will be updated.

        At variance, if `dynamic` is set to `False` the `manager_config` option will
        only be set for the `test_job` and not for the generated Jobs.
        """
        if dynamic:
            dict_input = {
                "config": config,
                "name_filter": name_filter,
                "function_filter": function_filter,
                "attributes": attributes,
            }
            self.config_updates.append(dict_input)

        # unwrap the functions in case the job is a decorated one
        function_filter = getattr(function_filter, "__wrapped__", function_filter)
        function = getattr(self.function, "__wrapped__", self.function)

        # if function_filter is not None and function_filter != self.function:
        if function_filter is not None and function_filter != function:
            return

        if name_filter is not None and (
            self.name is None or name_filter not in self.name
        ):
            return

        # if we get to here then we pass all the filters
        if isinstance(config, dict):
            # convert dict specification to a JobConfig but set the attributes
            if attributes is None:
                attributes = list(config.keys())

            attributes = [attributes] if isinstance(attributes, str) else attributes
            if not set(attributes).issubset(set(config.keys())):
                raise ValueError(
                    "Specified attributes include a key that is not present in the "
                    "config dictionary."
                )
            config = JobConfig(**config)

        if attributes is None:
            # overwrite the whole config
            self.config = config
        else:
            # only update the specified attributes
            attributes = [attributes] if isinstance(attributes, str) else attributes
            for attr in attributes:
                if not hasattr(self.config, attr):
                    raise ValueError(f"Unknown JobConfig attribute: {attr}")
                setattr(self.config, attr, getattr(config, attr))

    def as_dict(self) -> dict:
        """Serialize the job as a dictionary."""
        d = super().as_dict()

        # fireworks can't serialize functions and classes, so explicitly serialize to
        # the job recursively using monty to avoid issues
        return jsanitize(d, strict=True, enum_values=True, allow_bson=True)

    def __setattr__(self, key, value):
        """Handle setting attributes. Implements a special case for job name."""
        if key == "name" and value is not None and self.maker is not None:
            # have to be careful and also update the name of the bound maker
            # the ``value is not None`` in the if statement is needed otherwise the name
            # of the maker will get set to None during class init
            self.__dict__[key] = value
            self.maker.name = value
        else:
            super().__setattr__(key, value)

    def add_hosts_uuids(self, hosts_uuids: str | Sequence[str], prepend: bool = False):
        """
        Add a list of UUIDs to the internal list of hosts.

        The elements of the list are supposed to be ordered in such a way that
        the object identified by one UUID of the list is contained in objects
        identified by its subsequent elements.

        Parameters
        ----------
        hosts_uuids
            A list of UUIDs to add.
        prepend
            Insert the UUIDs at the beginning of the list rather than extending it.
        """
        if not isinstance(hosts_uuids, (list, tuple)):
            hosts_uuids = [hosts_uuids]  # type: ignore
        if prepend:
            self.hosts[0:0] = hosts_uuids
        else:
            self.hosts.extend(hosts_uuids)


# For type checking, the Response output type can be specified
# in a type hint via this type variable.
# For example, a signature `-> Response[int]` would require
# that the Response.output is an int.
T = typing.TypeVar("T")


@dataclass
class Response(typing.Generic[T]):
    """
    The :obj:`Response` contains the output, detours, and stop commands of a job.

    Parameters
    ----------
    output
        The job output.
    detour
        A flow or job to detour to.
    addition
        A flow or job to add to the current flow.
    replace
        A flow or job to replace the current job.
    stored_data
        Data to be stored by the flow manager.
    stop_children
        Stop any children of the current flow.
    stop_jobflow
        Stop executing all remaining jobs.
    """

    output: T = None
    detour: jobflow.Flow | Job | list[Job] | list[jobflow.Flow] = None
    addition: jobflow.Flow | Job | list[Job] | list[jobflow.Flow] = None
    replace: jobflow.Flow | Job | list[Job] | list[jobflow.Flow] = None
    stored_data: dict[Hashable, Any] = None
    stop_children: bool = False
    stop_jobflow: bool = False

    @classmethod
    def from_job_returns(
        cls,
        job_returns: Any | None,
        output_schema: type[BaseModel] = None,
    ) -> Response:
        """
        Generate a :obj:`Response` from the outputs of a :obj:`Job`.

        Parameters
        ----------
        job_returns
            The outputs of a job. If this is a :obj:`Response` object, the output schema
            will be applied to the response outputs and the response returned.
            Otherwise, the ``job_returns`` will be put into the ``outputs`` of a new
            :obj:`Response` object.
        output_schema
            A pydantic model associated with the job. Used to enforce a schema for the
            outputs.

        Raises
        ------
        ValueError
            If the job outputs do not match the output schema.

        Returns
        -------
        Response
            The job response controlling the data to store and flow execution options.
        """
        if isinstance(job_returns, Response):
            if job_returns.replace is None:
                # only apply output schema if there is no replace.
                job_returns.output = apply_schema(job_returns.output, output_schema)

            return job_returns

        if isinstance(job_returns, (list, tuple)):
            # check that a Response object is not given as one of many outputs
            for r in job_returns:
                if isinstance(r, Response):
                    raise ValueError(
                        "Response cannot be returned in combination with other outputs."
                    )

        return cls(output=apply_schema(job_returns, output_schema))


def apply_schema(output: Any, schema: type[BaseModel] | None):
    """
    Apply schema to job outputs.

    Parameters
    ----------
    output
        The job outputs.
    schema
        A pydantic model that defines the schema to apply.

    Raises
    ------
    ValueError
        If a schema is set but there are no outputs.
    ValueError
        If the outputs do not match the schema.

    Returns
    -------
    BaseModel or Any
        Returns an instance of the schema if the schema is set or the original output.
    """
    if schema is None or isinstance(output, schema):
        return output

    if output is None:
        raise ValueError(f"Expected output of type {schema.__name__} but got no output")

    if not isinstance(output, dict):
        raise ValueError(
            f"Expected output to be {schema.__name__} or dict but got output type "
            f"of {type(output).__name__}."
        )

    return schema(**output)


@job(config=JobConfig(resolve_references=False, on_missing_references=OnMissing.NONE))
def store_inputs(inputs: Any) -> Any:
    """
    Job to store inputs.

    Note that any :obj:`Reference` objects will not be resolved, however, missing
    references will be replaced with ``None``.

    Parameters
    ----------
    inputs:
        The inputs to store.
    """
    return inputs


def prepare_replace(
    replace: jobflow.Flow | Job | list[Job],
    current_job: Job,
) -> jobflow.Flow:
    """
    Prepare a replacement :obj:`Flow` or :obj:`Job`.

    If the replacement is a ``Flow``, then an additional ``Job`` will be inserted
    that maps the output id of the original job to outputs of the ``Flow``.

    If the replacement is a ``Flow`` or a ``Job``, then this function pass on
    the manager config, schema, and metadata and set the according UUIDs and job index.

    Parameters
    ----------
    replace
        A :obj:`Flow` or :obj:`Job` to use as the replacement.
    current_job
        The current job.

    Returns
    -------
    Flow
        The updated flow.
    """
    from jobflow.core.flow import Flow

    if isinstance(replace, (list, tuple)):
        replace = Flow(jobs=replace)

    if isinstance(replace, Flow) and replace.output is not None:
        # add a job with same UUID as the current job to store the outputs of the
        # flow; this job will inherit the metadata and output schema of the current
        # job
        store_output_job = store_inputs(replace.output)
        store_output_job.set_uuid(current_job.uuid)
        store_output_job.index = current_job.index + 1
        store_output_job.metadata = current_job.metadata
        store_output_job.output_schema = current_job.output_schema
        replace.add_jobs(store_output_job)

    elif isinstance(replace, Job):
        # replace is a single Job
        replace.set_uuid(current_job.uuid)
        replace.index = current_job.index + 1

        metadata = replace.metadata
        metadata.update(current_job.metadata)
        replace.metadata = metadata

        if not replace.output_schema:
            replace.output_schema = current_job.output_schema

        replace = Flow(jobs=replace, output=replace.output)

    return replace


def pass_manager_config(
    jobs: Job | jobflow.Flow | list[Job | jobflow.Flow],
    manager_config: dict[str, Any],
):
    """
    Pass the manager config on to any jobs in the jobs array.

    Parameters
    ----------
    jobs
        A job, flow, or list of jobs/flows.
    manager_config
        A manager config to pass on.
    metadata
        Metadata to pass on.
    """
    from copy import deepcopy

    all_jobs: list[Job] = []

    def get_jobs(arg):
        if isinstance(arg, Job):
            all_jobs.append(arg)
        elif isinstance(arg, (list, tuple)):
            for j in arg:
                get_jobs(j)
        elif hasattr(arg, "jobs"):
            # this is a flow
            get_jobs(arg.jobs)
        else:
            raise ValueError("Unrecognised jobs format")

    # extract all jobs from the input array
    get_jobs(jobs)

    # update manager config
    for ajob in all_jobs:
        ajob.config.manager_config = deepcopy(manager_config)
