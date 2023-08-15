"""Define base Flow object."""

from __future__ import annotations

import logging
import warnings
from copy import deepcopy
from typing import TYPE_CHECKING, Sequence

from monty.json import MSONable

import jobflow
from jobflow.core.reference import find_and_get_references
from jobflow.utils import ValueEnum, contains_flow_or_job, suuid

if TYPE_CHECKING:
    from typing import Any, Callable, Iterator

    from networkx import DiGraph

    from jobflow import Job

__all__ = ["JobOrder", "Flow", "get_flow"]

logger = logging.getLogger(__name__)


class JobOrder(ValueEnum):
    """
    Options to control the order of job execution.

    - ``AUTO``: Automatically determine the job order based on input and output
      references.
    - ``LINEAR``: Run the jobs in the order they appear in the jobs array.
    """

    AUTO = "auto"
    LINEAR = "linear"


class Flow(MSONable):
    """
    A Flow contains a collection of Jobs or other Flows to execute.

    The :obj:`Flow` object is the main tool for constructing workflows. Flows
    can either contain Jobs or other Flows. Like :obj:`.Job` objects, Flow objects
    can also have outputs, however, these are not explicitly stored in the database.
    Instead, the outputs of a Flow act to structure the outputs of the jobs
    contained within the Flow.

    Parameters
    ----------
    jobs
        The jobs to be run as a list of :obj:`.Job` or :obj:`Flow` objects.
    output
        The output of the flow. These should come from the output of one or more
        of the jobs.
    name
        The flow name.
    order
        The order in which the jobs should be executed. The default is to determine
        the order automatically based on the connections between jobs.
    uuid
        The identifier of the flow. This is generated automatically.
    hosts
        The list of UUIDs of the hosts containing the job. This is updated
        automatically when a flow is included in the jobs array of another flow.
        The object identified by one UUID of the list should be contained in objects
        identified by its subsequent elements.

    Raises
    ------
    ValueError
        If a job in the ``jobs`` array is already part of another flow.
    ValueError
        If any jobs needed to resolve the inputs of all jobs in the ``jobs`` array are
        missing.
    ValueError
        If any jobs needed to resolve the flow ``output`` are missing.

    Warns
    -----
    UserWarning
        If a ``.Job`` or ``Flow`` object is used as the Flow ``output`` rather
        than an ``OutputReference``.

    See Also
    --------
    .job, .Job, JobOrder

    Examples
    --------
    Below we define a simple job to add two numbers, and create an flow containing
    two connected add jobs.

    >>> from jobflow import job, Flow
    >>> @job
    ... def add(a, b):
    ...     return a + b
    >>> add_first = add(1, 2)
    >>> add_second = add(add_first.output, 2)
    >>> flow = Flow(jobs=[add_first, add_second])

    This flow does not expose any of the outputs of the jobs contained within it.
    We could instead "register" the output of the second add as the output of the
    flow.

    >>> flow = Flow(jobs=[add_first, add_second], output=add_second.output)

    This will allow the flow to be used in another flow. In this way, Flows
    can be infinitely nested. For example:

    >>> add_third = add(flow.output, 5)
    >>> outer_flow = Flow(jobs=[flow, add_third])

    Flows can be run using an flow manager. These enable running Flows
    locally or on compute clusters (using the FireWorks manager).

    >>> from jobflow.managers.local import run_locally
    >>> response = run_locally(flow)
    """

    def __init__(
        self,
        jobs: list[Flow | jobflow.Job] | jobflow.Job | Flow,
        output: Any = None,
        name: str = "Flow",
        order: JobOrder = JobOrder.AUTO,
        uuid: str = None,
        hosts: list[str] = None,
    ):
        from jobflow.core.job import Job

        if isinstance(jobs, (Job, Flow)):
            jobs = [jobs]

        if uuid is None:
            uuid = suuid()

        self.name = name
        self.order = order
        self.uuid = uuid
        self.hosts = hosts or []

        self._jobs: tuple[Flow | Job, ...] = ()
        self.add_jobs(jobs)
        self.output = output

    def __len__(self) -> int:
        """Get the number of jobs or subflows in the flow."""
        return len(self.jobs)

    def __getitem__(self, idx: int | slice) -> Flow | Job | tuple[Flow | Job, ...]:
        """Get the job(s) or subflow(s) at the given index/slice."""
        return self.jobs[idx]

    def __setitem__(
        self, idx: int | slice, value: Flow | Job | Sequence[Flow | Job]
    ) -> None:
        """Set the job(s) or subflow(s) at the given index/slice."""
        if (
            not isinstance(value, (Flow, jobflow.Job, tuple, list))
            or isinstance(value, (tuple, list))
            and not all(isinstance(v, (Flow, jobflow.Job)) for v in value)
        ):
            raise TypeError(
                f"Flow can only contain Job or Flow objects, not {type(value).__name__}"
            )
        jobs = list(self.jobs)
        jobs[idx] = value  # type: ignore[index, assignment]
        self.jobs = tuple(jobs)

    def __iter__(self) -> Iterator[Flow | Job]:
        """Iterate through the jobs in the flow."""
        return iter(self.jobs)

    def __contains__(self, item: Flow | Job) -> bool:
        """Check if the flow contains a job or subflow."""
        return item in self.jobs

    def __add__(self, other: Job | Flow | Sequence[Flow | Job]) -> Flow:
        """Add a job or subflow to the flow."""
        if not isinstance(other, (Flow, jobflow.Job, tuple, list)):
            return NotImplemented
        new_flow = deepcopy(self)
        new_flow.add_jobs(other)
        return new_flow

    def __sub__(self, other: Flow | Job) -> Flow:
        """Remove a job or subflow from the flow."""
        if other not in self.jobs:
            raise ValueError(f"{other!r} not found in flow")
        new_flow = deepcopy(self)
        new_flow.jobs = tuple([job for job in new_flow.jobs if job != other])
        return new_flow

    def __repr__(self, level: int = 0, prefix: str = "") -> str:
        """Get a string representation of the flow."""
        indent = "  " * level
        name, uuid = self.name, self.uuid
        _prefix = f"{prefix}." if prefix else ""
        job_reprs = "\n".join(
            f"{indent}{_prefix}{i}. "
            f"{j.__repr__(level + 1, f'{_prefix}{i}') if isinstance(j, Flow) else j}"
            for i, j in enumerate(self.jobs, 1)
        )
        return f"Flow({name=}, {uuid=})\n{job_reprs}"

    def __eq__(self, other: object) -> bool:
        """Check if the flow is equal to another flow."""
        if not isinstance(other, Flow):
            return NotImplemented
        return self.uuid == other.uuid

    def __hash__(self) -> int:
        """Get the hash of the flow."""
        return hash(self.uuid)

    @property
    def jobs(self) -> tuple[Flow | Job, ...]:
        """
        Get the Jobs in the Flow.

        Returns
        -------
        list[Job]
            The list of Jobs/Flows of the Flow.
        """
        return self._jobs

    @jobs.setter
    def jobs(self, jobs: Sequence[Flow | Job] | Job | Flow):
        """
        Set the Jobs in the Flow.

        Parameters
        ----------
        jobs
            The list of Jobs/Flows of the Flow.
        """
        if isinstance(jobs, (Flow, jobflow.Job)):
            jobs = [jobs]
        self._jobs = tuple(jobs)

    @property
    def output(self) -> Any:
        """
        Get the output of the flow.

        Returns
        -------
        Any
            The output of the flow.
        """
        return self._output

    @output.setter
    def output(self, output: Any):
        """
        Set the output of the Flow.

        The output should be compatible with the list of Jobs/Flows contained in the
        Flow.

        Parameters
        ----------
        output
            The output of the flow. These should come from the output of one
            or more of the jobs.
        """
        if output is not None:
            if contains_flow_or_job(output):
                warnings.warn(
                    f"Flow '{self.name}' contains a Flow or Job as an output. "
                    f"Usually the Flow output should be the output of a Job or "
                    f"another Flow (e.g. job.output). If this message is "
                    f"unexpected then double check the outputs of your Flow."
                )

            # check if the jobs array contains all jobs needed for the references
            references = find_and_get_references(output)
            reference_uuids = {ref.uuid for ref in references}

            if not reference_uuids.issubset(set(self.job_uuids)):
                raise ValueError(
                    "jobs array does not contain all jobs needed for flow output"
                )
        self._output = output

    @property
    def job_uuids(self) -> tuple[str, ...]:
        """
        Uuids of every Job contained in the Flow (including nested Flows).

        Returns
        -------
        tuple[str]
            The uuids of all Jobs in the Flow (including nested Flows).
        """
        uuids: list[str] = []
        for job in self.jobs:
            if isinstance(job, Flow):
                uuids.extend(job.job_uuids)
            else:
                uuids.append(job.uuid)
        return tuple(uuids)

    @property
    def all_uuids(self) -> tuple[str, ...]:
        """
        Uuids of every Job and Flow contained in the Flow (including nested Flows).

        Returns
        -------
        tuple[str]
            The uuids of all Jobs and Flows in the Flow (including nested Flows).
        """
        uuids: list[str] = []
        for job in self.jobs:
            if isinstance(job, Flow):
                uuids.extend(job.all_uuids)
            uuids.append(job.uuid)
        return tuple(uuids)

    @property
    def graph(self) -> DiGraph:
        """
        Get a graph indicating the connectivity of jobs in the flow.

        Returns
        -------
        DiGraph
            The graph showing the connectivity of the jobs.
        """
        from itertools import product

        import networkx as nx

        graph = nx.compose_all([job.graph for job in self.jobs])

        for node in graph:
            node_props = graph.nodes[node]
            if all(k not in node_props for k in ("job", "label")):
                nx.set_node_attributes(graph, {node: {"label": "external"}})

        if self.order == JobOrder.LINEAR:
            # add fake edges between jobs to force linear order
            edges = []
            for job_a, job_b in nx.utils.pairwise(self.jobs):
                if isinstance(job_a, Flow):
                    leaves = [v for v, d in job_a.graph.out_degree() if d == 0]
                else:
                    leaves = [job_a.uuid]

                if isinstance(job_b, Flow):
                    roots = [v for v, d in job_b.graph.in_degree() if d == 0]
                else:
                    roots = [job_b.uuid]

                for leaf, root in product(leaves, roots):
                    edges.append((leaf, root, {"properties": ""}))
            graph.add_edges_from(edges)
        return graph

    @property
    def host(self) -> str | None:
        """
        UUID of the first Flow that contains this Flow.

        Returns
        -------
        str
            the UUID of the host.
        """
        return self.hosts[0] if self.hosts else None

    def draw_graph(self, **kwargs):
        """
        Draw the flow graph using matplotlib.

        Requires matplotlib to be installed.

        Parameters
        ----------
        kwargs
            keyword arguments that are passed to :obj:`jobflow.utils.graph.draw_graph`.

        Returns
        -------
        pyplot
            The matplotlib pyplot state object.
        """
        from jobflow.utils.graph import draw_graph

        return draw_graph(self.graph, **kwargs)

    def iterflow(self):
        """
        Iterate through the jobs of the flow.

        The jobs are yielded such that the job output references can always be
        resolved. I.e., root nodes of the flow graph are always returned first.

        Yields
        ------
        Job, list[str]
            The Job and the uuids of any parent jobs (not to be confused with the host
            flow).
        """
        from networkx import is_directed_acyclic_graph

        from jobflow.utils.graph import itergraph

        graph = self.graph

        if not is_directed_acyclic_graph(graph):
            raise ValueError(
                "Job connectivity contains cycles therefore job execution order "
                "cannot be determined."
            )

        for node in itergraph(graph):
            parents = [u for u, v in graph.in_edges(node) if "job" in graph.nodes[u]]
            if "job" not in graph.nodes[node]:
                continue
            job = graph.nodes[node]["job"]
            yield job, parents

    def update_kwargs(
        self,
        update: dict[str, Any],
        name_filter: str = None,
        function_filter: Callable = None,
        dict_mod: bool = False,
    ):
        """
        Update the kwargs of all Jobs in the Flow.

        Note that updates will be applied to jobs in nested Flow.

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
        Consider a flow containing a simple job with a ``number`` keyword argument.

        >>> from jobflow import job, Flow
        >>> @job
        ... def add(a, number=5):
        ...     return a + number
        >>> add_job = add(1)
        >>> flow = Flow([add_job])

        The ``number`` argument could be updated in the following ways.

        >>> flow.update_kwargs({"number": 10})

        This will work if all jobs in the flow have a kwarg called number. However,
        when this is not the case this will result in the bad input kwargs for some
        jobs. To only apply the update to the correct jobs, filters can be used.

        >>> flow.update_kwargs({"number": 10}, name_filter="add")
        >>> flow.update_kwargs({"number": 10}, function_filter=add)
        """
        for job in self.jobs:
            job.update_kwargs(
                update,
                name_filter=name_filter,
                function_filter=function_filter,
                dict_mod=dict_mod,
            )

    def update_maker_kwargs(
        self,
        update: dict[str, Any],
        name_filter: str = None,
        class_filter: type[jobflow.Maker] = None,
        nested: bool = True,
        dict_mod: bool = False,
    ):
        """
        Update the keyword arguments of any :obj:`.Maker` objects in the jobs.

        Note that updates will be applied to Jobs in any inner Flows.

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
        Consider the following flow containing jobs from a Maker:

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
        >>> flow = Flow([add_job])

        The ``number`` argument could be updated in the following ways.

        >>> flow.update_maker_kwargs({"number": 10})

        This will work if all Makers in the flow have a kwarg called number.
        However, when this is not the case this will result in the bad input kwargs
        for some Makers. To only apply the update to the correct Makers, filters can be
        used.

        >>> flow.update_maker_kwargs({"number": 10}, name_filter="add")
        >>> flow.update_maker_kwargs({"number": 10}, class_filter=AddMaker)

        By default, the updates are applied to nested Makers. These are Makers
        which are present in the kwargs of another Maker. Consider the following case
        for a Maker that produces a job that restarts.

        >>> from jobflow import Response
        >>> @dataclass
        ... class RestartMaker(Maker):
        ...     name: str = "replace"
        ...     add_maker: Maker = AddMaker()
        ...
        ...     @job
        ...     def make(self, a):
        ...         restart_job = self.add_maker.make(a)
        ...         return Response(replace=restart_job)
        >>> maker = RestartMaker()
        >>> my_job = maker.make(1)
        >>> flow = Flow([my_job]

        The following update will apply to the nested ``AddMaker`` in the kwargs of the
        ``RestartMaker``:

        >>> flow.update_maker_kwargs({"number": 10}, class_filter=AddMaker)

        However, if ``nested=False``, then the update will not be applied to the nested
        Maker:

        >>> flow.update_maker_kwargs(
        ...     {"number": 10}, class_filter=AddMaker, nested=False
        ... )
        """
        for job in self.jobs:
            job.update_maker_kwargs(
                update,
                name_filter=name_filter,
                class_filter=class_filter,
                nested=nested,
                dict_mod=dict_mod,
            )

    def append_name(self, append_str: str, prepend: bool = False):
        """
        Append a string to the name of the flow and all jobs contained in it.

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

        for job in self.jobs:
            job.append_name(append_str, prepend=prepend)

    def update_metadata(
        self,
        update: dict[str, Any],
        name_filter: str = None,
        function_filter: Callable = None,
        dict_mod: bool = False,
        dynamic: bool = True,
    ):
        """
        Update the metadata of all Jobs in the Flow.

        Note that updates will be applied to jobs in nested Flow.

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
        Consider a flow containing two jobs.

        >>> from jobflow import job, Flow
        >>> @job
        ... def add(a, b):
        ...     return a + b
        >>> add_job1 = add(5, 6)
        >>> add_job2 = add(6, 7)
        >>> flow = Flow([add_job1, add_job2])

        The ``metadata`` of both jobs could be updated as follows:

        >>> flow.update_metadata({"tag": "addition_job"})
        """
        for job in self.jobs:
            job.update_metadata(
                update,
                name_filter=name_filter,
                function_filter=function_filter,
                dict_mod=dict_mod,
                dynamic=dynamic,
            )

    def update_config(
        self,
        config: jobflow.JobConfig | dict,
        name_filter: str = None,
        function_filter: Callable = None,
        attributes: list[str] | str = None,
        dynamic: bool = True,
    ):
        """
        Update the job config of all Jobs in the Flow.

        Note that updates will be applied to jobs in nested Flow.

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
        Consider a flow containing two jobs.

        >>> from jobflow import job, Flow
        >>> @job
        ... def add(a, b):
        ...     return a + b
        >>> add_job1 = add(5, 6)
        >>> add_job2 = add(6, 7)
        >>> flow = Flow([add_job1, add_job2])

        The ``config`` of both jobs could be updated as follows:

        >>> new_config = JobConfig(
        ...    manager_config={"_fworker": "myfworker"}, resolve_references=False
        ... )
        >>> flow.update_config(new_config)

        To only update specific attributes, the ``attributes`` argument can be set. For
        example, the following will only update the "manager_config" attribute of the
        jobs' config.

        >>> flow.update_config(new_config, attributes="manager_config")

        Alternatively, the config can be specified as a dictionary with keys that are
        attributes of the JobConfig object. This allows you to specify updates without
        having to create a completely new JobConfig object. For example:

        >>> flow.update_config({"manager_config": {"_fworker": "myfworker"}})
        """
        for job in self.jobs:
            job.update_config(
                config,
                name_filter=name_filter,
                function_filter=function_filter,
                attributes=attributes,
                dynamic=dynamic,
            )

    def add_hosts_uuids(
        self, hosts_uuids: str | list[str] = None, prepend: bool = False
    ):
        """
        Add a list of UUIDs to the internal list of hosts.

        If hosts_uuids is None the uuid of this Flow will be added to the inner jobs and
        flow. Otherwise, the passed value will be set both in the list of hosts
        of the current flow and of the inner jobs and flows.
        The elements of the list are supposed to be ordered in such a way that
        the object identified by one UUID of the list is contained in objects
        identified by its subsequent elements.

        Parameters
        ----------
        hosts_uuids
            A list of UUIDs to add. If None the current uuid of the flow will be
            added to the inner Flows and Jobs.
        prepend
            Insert the UUIDs at the beginning of the list rather than extending it.
        """
        if hosts_uuids is not None:
            if not isinstance(hosts_uuids, (list, tuple)):
                hosts_uuids = [hosts_uuids]
            if prepend:
                self.hosts[0:0] = hosts_uuids
            else:
                self.hosts.extend(hosts_uuids)
        else:
            hosts_uuids = [self.uuid]
        for j in self.jobs:
            j.add_hosts_uuids(hosts_uuids, prepend=prepend)

    def add_jobs(self, jobs: Job | Flow | Sequence[Flow | Job]) -> None:
        """
        Add Jobs or Flows to the Flow.

        Added objects should not belong to other flows. The list of hosts will be added
        automatically to the incoming Jobs/Flows based on the hosts of the current Flow.

        Parameters
        ----------
        jobs
            A list of Jobs and Flows.
        """
        if not isinstance(jobs, (tuple, list)):
            jobs = [jobs]  # type: ignore[list-item]

        job_ids = set(self.all_uuids)
        hosts = [self.uuid, *self.hosts]
        for job in jobs:
            if job.host is not None and job.host != self.uuid:
                raise ValueError(
                    f"{type(job).__name__} {job.name} ({job.uuid}) already belongs "
                    f"to another flow."
                )
            if job.uuid in job_ids:
                raise ValueError(
                    "jobs array contains multiple jobs/flows with the same uuid "
                    f"({job.uuid})"
                )
            # check for circular dependency of Flows.
            if isinstance(job, Flow) and self.uuid in job.all_uuids:
                raise ValueError(
                    f"circular dependency: Flow ({job.uuid}) contains the "
                    f"current Flow ({self.uuid})"
                )
            job_ids.add(job.uuid)
            job.add_hosts_uuids(hosts)
        self._jobs += tuple(jobs)

    def remove_jobs(self, indices: int | list[int]):
        """
        Remove jobs from the Flow.

        It is not possible to remove jobs referenced in the output.

        Parameters
        ----------
        indices
            Indices of the jobs to be removed. Accepted values: from 0 to len(jobs) - 1.
        """
        if not isinstance(indices, (list, tuple)):
            indices = [indices]
        if any(i < 0 or i >= len(self.jobs) for i in indices):
            raise ValueError(
                "Only indices between 0 and the number of the jobs are accepted"
            )

        new_jobs = tuple(j for i, j in enumerate(self.jobs) if i not in indices)
        uuids: set = set()
        for job in new_jobs:
            if isinstance(job, Flow):
                uuids.update(job.job_uuids)
            else:
                uuids.add(job.uuid)

        # check if the output contains some references to the removed Jobs.
        references = find_and_get_references(self.output)
        reference_uuids = {ref.uuid for ref in references}

        if not reference_uuids.issubset(uuids):
            raise ValueError(
                "Removed Jobs/Flows are referenced in the output of the Flow."
            )

        self._jobs = new_jobs


def get_flow(
    flow: Flow | Job | list[jobflow.Job],
    allow_external_references: bool = False,
) -> Flow:
    """
    Check dependencies and return flow object.

    Parameters
    ----------
    flow
        A job, list of jobs, or flow.
    allow_external_references
        If False all the references to other outputs should be from other Jobs
        of the Flow.

    Returns
    -------
    Flow
        A :obj:`Flow` object where connections have been checked.
    """
    if not isinstance(flow, Flow):
        flow = Flow(jobs=flow)

    if not allow_external_references:
        # ensure that we have all the jobs needed to resolve the reference connections
        job_references = find_and_get_references(flow.jobs)
        job_reference_uuids = {ref.uuid for ref in job_references}
        missing_jobs = job_reference_uuids.difference(set(flow.job_uuids))
        if len(missing_jobs) > 0:
            raise ValueError(
                "The following jobs were not found in the jobs array and are needed to "
                f"resolve output references:\n{list(missing_jobs)}\nIf the references "
                "are from external jobs and this is the intended behavior set "
                "allow_external_references to True"
            )

    return flow
