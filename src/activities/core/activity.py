"""Define base Activity object."""
from __future__ import annotations

import logging
import typing
from dataclasses import dataclass, field

from monty.json import MSONable

from activities.core.util import ValueEnum

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
    from uuid import UUID

    from networkx import DiGraph

    import activities


logger = logging.getLogger(__name__)


class JobOrder(ValueEnum):
    AUTO = "auto"
    LINEAR = "linear"


@dataclass
class Activity(MSONable):
    """
    An Activity contains a sequence of Tasks or other Activities to execute.

    The :obj:`Activity: object is the main tool for constructing workflows. Activities
    can either contain tasks or other activities but not a mixture of both.
    Like :obj:`Job` objects, activities can also have outputs. The outputs
    of an activity will likely be stored in a database (depending on the manager used
    to run the activity), whereas the outputs of tasks are only available while the
    activity is running.

    .. Note::
        There is one important difference between activities containing :obj:`Job`
        objects and those containing other :obj:`Activity` objects: Activities
        containing :obj:`Job` objects will execute the tasks in the order they are
        given in the ``tasks`` list, whereas activities containing :obj:`Activity`
        objects sorted to determine the optimal execution order.

        This may be changed in a future release.

    Parameters
    ----------
    name
        The activity name.
    tasks
        The tasks to be run. Can either be a list of :obj:`Job` objects or a list of
        :obj:`Activity` objects.
    outputs
        The outputs of the activity. These should come from the outputs of one or more
        of the tasks contained in the activity.
    config
        A config dictionary for controlling the execution of the activity.
    host
        The identifier of the host activity. This is set automatically when an activity
        is included in the tasks of another activity.
    uuid
        A unique identifier for the activity. Generated automatically.
    output_source
        The sources of the output of the activity. Set automatically.

    Examples
    --------

    Below we define a simple job to add two numbers, and create an activity containing
    that job.

    >>> from activities import job, Activity
    ...
    >>> @job
    ... def add(a, b):
    ...     return a + b
    ...
    >>> add_task = add(1, 2)
    >>> activity = Activity(tasks=[add_task])

    If we were to run this activity, what would happen to the output of the job? It
    would be lost as the outputs of the activity was not defined. To remedy that, we
    can set the outputs of the activity to be the outputs of the ``add_task`` job.

    >>> activity = Activity(tasks=[add_task], outputs=add_task.outputs)

    If we run the activity, we get an :obj:`ActivityResponse` object, that contains the
    outputs among other things.

    >>> response = activity.run()
    >>> response.outputs
    Value(value=3)

    It is not recommended to run the activity directly as we have done above. Instead,
    we provide several activity managers for running activities locally or remotely.

    >>> from activities.managers.local import run_locally
    >>> response = run_locally(activity)

    The outputs of activities can be used by other activities. Note also that
    activities can contain activities.

    >>> task1 = add(1, 2)
    >>> activity1 = Activity(tasks=[task1], outputs=task1.outputs)
    ...
    ... # use the outputs of the activity in another activity
    >>> task2 = add(activity1.outputs.value, 5)
    >>> activity2 = Activity(tasks=[task2], outputs=task2.outputs)
    ...
    ... # now create an activity containing other activities
    >>> activity = Activity(tasks=[activity1, activity2], outputs=activity2.outputs)

    An activity cannot contain both tasks and activities simulatenously.

    >>> activity = Activity(tasks=[task1, activity2])
    ValueError("Cannot mix Activity objects and Job objects in the same Activity")

    By defining the job output class, activities can make use of static
    parameter checking to ensure that connections between tasks are valid.

    >>> from activities.core.outputs import Number
    >>> @job(outputs=Number)
    ... def add(a, b):
    ...     return Number(a + b)
    ...
    ... task1 = add(1, 2)
    ... task2 = add(task1.outputs.bad_output, 5)
    AttributeError: 'Number' object has no attribute 'bad_output'
    """

    jobs: Union[List[Union[Activity, activities.Job]], activities.Job] = field(
        default_factory=list
    )
    output: Optional[Any] = None
    order: JobOrder = JobOrder.AUTO

    def __post_init__(self):
        from activities import Job

        if isinstance(self.jobs, Job):
            self.jobs = [self.jobs]

    @property
    def graph(self) -> DiGraph:
        import networkx as nx

        graph = nx.compose_all([job.graph for job in self.jobs])

        if self.order == JobOrder.LINEAR:
            # add fake edges between jobs to force linear order
            edges = []
            for job_a, job_b in nx.utils.pairwise(self.jobs):
                edges.append((job_a.uuid, job_b.uuid, {"properties": ""}))
            graph.add_edges_from(edges)
        return graph

    def iteractivity(
        self,
    ) -> Generator[Tuple["activities.Job", List[UUID]], None, None]:
        from activities.core.graph import itergraph

        graph = self.graph
        for node in itergraph(graph):
            parents = [u for u, v in graph.in_edges(node)]
            job = graph.nodes[node]["job"]
            yield job, parents

    def draw_graph(self):
        from activities.core.graph import draw_graph

        return draw_graph(self.graph)

    def update_kwargs(
        self,
        update: Dict[str, Any],
        name_filter: Optional[str] = None,
        function_filter: Optional[Callable] = None,
        dict_mod: bool = False,
    ):
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
        for job in self.jobs:
            job.update_maker_kwargs(
                update,
                name_filter=name_filter,
                class_filter=class_filter,
                nested=nested,
                dict_mod=dict_mod,
            )
