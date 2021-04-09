"""Define base Activity object."""

from __future__ import annotations

import logging
import typing
import warnings

from monty.json import MSONable

from activities.core.util import ValueEnum, contains_activity_or_job, suuid

if typing.TYPE_CHECKING:
    from typing import (
        Any,
        Callable,
        Dict,
        Generator,
        List,
        Optional,
        Tuple,
        Type,
        Union,
    )

    from networkx import DiGraph

    import activities

__all__ = ["JobOrder", "Activity"]

logger = logging.getLogger(__name__)


class JobOrder(ValueEnum):
    AUTO = "auto"
    LINEAR = "linear"


class Activity(MSONable):
    """
    An Activity contains a collection of Jobs or other Activities to execute.

    The :obj:`Activity: object is the main tool for constructing workflows. Activities
    can either contain jobs or other activities. Like :obj:`Job` objects, activities
    can also have outputs, however, these are not explicitly stored in the database.
    Instead, the outputs of an Activity act to structure the outputs of the jobs
    contained within the activity.

    Parameters
    ----------
    jobs
        The jobs to be run as a list of :obj:`Job` or :obj:`Activity` objects.
    output
        The output of the activity. These should come from the output of one or more
        of the jobs.
    name
        The activity name.
    order
        The order in which the jobs should be exectuted. The default is to determine
        the order automatically based on the connections between jobs.
    uuid
        The identifier of the activity. This is genenrated automatically.
    host
        The identifier of the host activity. This is set automatically when an activity
        is included in the jobs array of another activity.

    Attributes
    ----------
    jobs
        The jobs to be run.
    output
        The output of the activity.
    name
        The activity name.
    order
        The order in which the jobs should be exectuted.
    uuid
        The identifier of the activity.
    host
        The identifier of the host activity.

    Raises
    ------
    ValueError
        If a job in the `jobs` array is already part of another activity.
    ValueError
        If any jobs needed to resolve the inputs of all jobs in the `jobs` array are
        missing.
    ValueError
        If any jobs needed to resolve the activity `output` are missing.

    Warns
    -----
    UserWarning
        If a `Job` or `Activity` object is used as the Activity `output` rather than
        an `OutputReference`.

    See Also
    --------
    .job, .Job, JobOrder

    Examples
    --------
    Below we define a simple job to add two numbers, and create an activity containing
    two connected add jobs.

    >>> from activities import job, Activity
    >>> @job
    ... def add(a, b):
    ...     return a + b
    >>> add_first = add(1, 2)
    >>> add_second = add(add_first.output, 2)
    >>> activity = Activity(jobs=[add_first, add_second])

    This activity does not expose any of the outputs of the jobs contained within it.
    We could instead "register" the output of the second add as the output of the
    activity.

    >>> activity = Activity(jobs=[add_first, add_second], output=add_second.output)

    This will the activity to be used in another activity. In this way, activities
    can be infinitely nested. For example:

    >>> add_third = add(activity.output, 5)
    >>> outer_activity = Activity(jobs=[activity, add_third])

    Activities can be run using an activity manager. These enable running activities
    locally or on compute clusters (using the FireWorks manager).

    >>> from activities.managers.local import run_locally
    >>> response = run_locally(activity)
    """

    def __init__(
        self,
        jobs: Union[List[Union[Activity, activities.Job]], activities.Job, Activity],
        output: Optional[Any] = None,
        name: str = "Activity",
        order: JobOrder = JobOrder.AUTO,
        uuid: str = None,
        host: str = None,
    ):
        from activities.core.job import Job
        from activities.core.reference import find_and_get_references

        if isinstance(jobs, (Job, Activity)):
            jobs = [jobs]

        if uuid is None:
            self.uuid = suuid()

        self.jobs = jobs
        self.output = output
        self.name = name
        self.order = order
        self.uuid = uuid
        self.host = host

        # ensure that we have all the jobs needed to resolve the reference connections
        job_references = find_and_get_references(self.jobs)
        job_reference_uuids = set([ref.uuid for ref in job_references])
        missing_jobs = job_reference_uuids.difference(set(self.job_uuids))
        if len(missing_jobs) > 0:
            raise ValueError(
                "The following jobs were not found in the jobs array and are needed to "
                f"resolve output references:\n{list(missing_jobs)}"
            )

        for job in self.jobs:
            if job.host is not None:
                raise ValueError(
                    f"{job.__class__.__name__} {job.name} ({job.uuid}) already belongs "
                    f"to another activity."
                )
            job.host = self.uuid

        if self.output is not None:
            if contains_activity_or_job(self.output):
                warnings.warn(
                    f"Activity '{self.name}' contains an Activity or Job as an output. "
                    f"Usually the Activity output should be the output of a Job or "
                    f"another Activity (e.g. job.output). If this message is "
                    f"unexpected then double check the outputs of your Activity."
                )

            # check if the jobs array contains all jobs needed for the references
            references = find_and_get_references(self.output)
            reference_uuids = set([ref.uuid for ref in references])

            if not reference_uuids.issubset(set(self.job_uuids)):
                raise ValueError(
                    "jobs array does not contain all jobs needed for activity output"
                )

    @property
    def graph(self) -> DiGraph:
        """
        Get a graph indicating the connectivity of jobs in the activity.

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
                if isinstance(job_a, Activity):
                    leaves = [v for v, d in job_a.graph.out_degree() if d == 0]
                else:
                    leaves = [job_a.uuid]

                if isinstance(job_b, Activity):
                    roots = [v for v, d in job_b.graph.in_degree() if d == 0]
                else:
                    roots = [job_b.uuid]

                for leaf, root in product(leaves, roots):
                    edges.append((leaf, root, {"properties": ""}))
            graph.add_edges_from(edges)
        return graph

    def iteractivity(
        self,
    ) -> Generator[Tuple["activities.Job", List[str]], None, None]:
        """
        Iterate through the jobs of the activity.

        The jobs are yielded such that the job output references can always be
        resolved. I.e., root nodes of the activity graph are always returned first.

        Yields
        -------
        (Job, list(str))
            The Job and the uuids of any parent jobs (not to be confused with the host
            activity).
        """
        from activities.core.graph import itergraph

        graph = self.graph
        for node in itergraph(graph):
            parents = [u for u, v in graph.in_edges(node)]
            job = graph.nodes[node]["job"]
            yield job, parents

    def draw_graph(self):
        """
        Draw the activity graph using matplotlib.

        Requires matplotlib to be installed.

        Returns
        -------
        pyplot
            The matplotlib pyplot state.
        """
        from activities.core.graph import draw_graph

        return draw_graph(self.graph)

    def update_kwargs(
        self,
        update: Dict[str, Any],
        name_filter: Optional[str] = None,
        function_filter: Optional[Callable] = None,
        dict_mod: bool = False,
    ):
        """
        Update the kwargs of all jobs in the activity .

        Note that updates will be applied to jobs in nested activities.

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
        Consider an activity containing a simple job with a `number` keyword argument.

        >>> from activities import job, Activity
        >>> @job
        ... def add(a, number=5):
        ...     return a + number
        >>> add_job = add(1)
        >>> activity = Activity([add_job])

        The `number` argument could be updated in the following ways.

        >>> activity.update_kwargs({"number": 10})

        This will work if all jobs in the activity have a kwarg called number. However,
        when this is not the case this will result in the bad input kwargs for some
        jobs. To only apply the update to the correct jobs, filters can be used.

        >>> activity.update_kwargs({"number": 10}, name_filter="add")
        >>> activity.update_kwargs({"number": 10}, function_filter=add)
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
        class_filter: Optional[Type[activities.Maker]] = None,
        nested: bool = True,
        dict_mod: bool = False,
    ):
        """
        Update the keyword arguments of any :obj:`.Maker` objects in the jobs.

        Note that updates will be applied to jobs in any inner activities.

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
            of a Maker object. See examples for more details.
        dict_mod
            Use the dict mod language to apply updates. See :obj:`.DictMods` for more
            details.

        Examples
        --------
        Consider the following activity containing jobs from a Maker:

        >>> from dataclasses import dataclass
        >>> from activities import job, Maker, Activity
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
        >>> activity = Activity([add_job])

        The `number` argument could be updated in the following ways.

        >>> activity.update_maker_kwargs({"number": 10})

        This will work if all Makers in the activity have a kwarg called number.
        However, when this is not the case this will result in the bad input kwargs
        for some Makers. To only apply the update to the correct Makers, filters can be
        used.

        >>> activity.update_maker_kwargs({"number": 10}, name_filder="add")
        >>> activity.update_maker_kwargs({"number": 10}, function_filter=AddMaker)

        By default, the updates are applied to nested Makers. These are Makers
        which are present in the kwargs of another Maker. Consider the following case
        for a Maker that produces a job that restarts.

        >>> from activities import Response
        >>> @dataclass
        ... class RestartMaker(Maker):
        ...     name: str = "restart"
        ...     add_maker: Maker = AddMaker()
        ...
        ...     @job
        ...     def make(self, a):
        ...         restart_job = self.add_maker.make(a)
        ...         return Response(restart=restart_job)
        >>> maker = RestartMaker()
        >>> activity = maker.make(1)

        The following update will apply to the nested `AddMaker` in the kwargs of the
        `RestartMaker`:

        >>> activity.update_maker_kwargs({"number": 10}, function_filter=AddMaker)

        However, if `nested=False`, then the update will not be applied to the nested
        Maker:

        >>> activity.update_maker_kwargs(
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

    @property
    def job_uuids(self) -> List[str]:
        """
        The uuids of every job contained in the activity (including nested activities).

        Returns
        -------
        list[str]
            The uuids of all jobs in the activity (including nested activities).
        """
        uuids = []
        for job in self.jobs:
            if isinstance(job, Activity):
                uuids.extend(job.job_uuids)
            else:
                uuids.append(job.uuid)
        return uuids
