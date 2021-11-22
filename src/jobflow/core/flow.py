"""Define base Flow object."""

from __future__ import annotations

import logging
import typing
import warnings

from monty.json import MSONable

from jobflow.core.reference import find_and_get_references
from jobflow.utils import ValueEnum, contains_flow_or_job, suuid

if typing.TYPE_CHECKING:
    from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

    from networkx import DiGraph

    import jobflow

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
        The order in which the jobs should be exectuted. The default is to determine
        the order automatically based on the connections between jobs.
    uuid
        The identifier of the flow. This is genenrated automatically.
    host
        The identifier of the host flow. This is set automatically when an flow
        is included in the jobs array of another flow.

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
        jobs: Union[List[Union[Flow, jobflow.Job]], jobflow.Job, Flow],
        output: Optional[Any] = None,
        name: str = "Flow",
        order: JobOrder = JobOrder.AUTO,
        uuid: str = None,
        host: str = None,
    ):
        from jobflow.core.job import Job
        from jobflow.core.reference import find_and_get_references

        if isinstance(jobs, (Job, Flow)):
            jobs = [jobs]

        if uuid is None:
            uuid = suuid()

        self.jobs = jobs
        self.output = output
        self.name = name
        self.order = order
        self.uuid = uuid
        self.host = host

        job_ids = set()
        for job in self.jobs:
            if job.host is not None and job.host != self.uuid:
                raise ValueError(
                    f"{job.__class__.__name__} {job.name} ({job.uuid}) already belongs "
                    f"to another flow."
                )
            if job.uuid in job_ids:
                raise ValueError(
                    "jobs array contains multiple jobs/flows with the same uuid "
                    f"({job.uuid})"
                )
            job.host = self.uuid
            job_ids.add(job.uuid)

        if self.output is not None:
            if contains_flow_or_job(self.output):
                warnings.warn(
                    f"Flow '{self.name}' contains a Flow or Job as an output. "
                    f"Usually the Flow output should be the output of a Job or "
                    f"another Flow (e.g. job.output). If this message is "
                    f"unexpected then double check the outputs of your Flow."
                )

            # check if the jobs array contains all jobs needed for the references
            references = find_and_get_references(self.output)
            reference_uuids = {ref.uuid for ref in references}

            if not reference_uuids.issubset(set(self.job_uuids)):
                raise ValueError(
                    "jobs array does not contain all jobs needed for flow output"
                )

    @property
    def job_uuids(self) -> Tuple[str, ...]:
        """
        Uuids of every Job contained in the Flow (including nested Flows).

        Returns
        -------
        list[str]
            The uuids of all Jobs in the Flow (including nested Flows).
        """
        uuids: List[str] = []
        for job in self.jobs:
            if isinstance(job, Flow):
                uuids.extend(job.job_uuids)
            else:
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

    def draw_graph(self, **kwargs):
        """
        Draw the flow graph using matplotlib.

        Requires matplotlib to be installed.

        Returns
        -------
        pyplot
            The matplotlib pyplot state object.
        kwargs
            keyword arguments that are passed to :obj:`jobflow.utils.graph.draw_graph`.
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
            parents = [u for u, v in graph.in_edges(node)]
            job = graph.nodes[node]["job"]
            yield job, parents

    def update_kwargs(
        self,
        update: Dict[str, Any],
        name_filter: Optional[str] = None,
        function_filter: Optional[Callable] = None,
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
            A filter for the job name.
        function_filter
            Only filter matching functions.
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
        update: Dict[str, Any],
        name_filter: Optional[str] = None,
        class_filter: Optional[Type[jobflow.Maker]] = None,
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
            A filter for the Maker name.
        class_filter
            A filter for the maker class. Note the class filter will match any
            subclasses.
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

        >>> flow.update_maker_kwargs({"number": 10}, name_filder="add")
        >>> flow.update_maker_kwargs({"number": 10}, function_filter=AddMaker)

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

        >>> flow.update_maker_kwargs({"number": 10}, function_filter=AddMaker)

        However, if ``nested=False``, then the update will not be applied to the nested
        Maker:

        >>> flow.update_maker_kwargs(
        ...     {"number": 10}, function_filter=AddMaker, nested=False
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


def get_flow(
    flow: Union[Flow, jobflow.Job, List[jobflow.Job]],
) -> Flow:
    """
    Check dependencies and return flow object.

    Parameters
    ----------
    flow
        A job, list of jobs, or flow.

    Returns
    -------
    Flow
        A :obj:`Flow` object where connections have been checked.
    """
    if not isinstance(flow, Flow):
        flow = Flow(jobs=flow)

    # ensure that we have all the jobs needed to resolve the reference connections
    job_references = find_and_get_references(flow.jobs)
    job_reference_uuids = {ref.uuid for ref in job_references}
    missing_jobs = job_reference_uuids.difference(set(flow.job_uuids))
    if len(missing_jobs) > 0:
        raise ValueError(
            "The following jobs were not found in the jobs array and are needed to "
            f"resolve output references:\n{list(missing_jobs)}"
        )

    return flow
