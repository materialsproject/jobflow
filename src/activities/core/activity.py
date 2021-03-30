"""Define base Activity object."""
from __future__ import annotations

import logging
import typing
from dataclasses import dataclass, field
from uuid import uuid4

from monty.json import MSONable

from activities.core.base import HasInputOutput
from activities.core.reference import Reference

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Generator, Optional, Sequence, Tuple, Type, Union
    from uuid import UUID

    from networkx import DiGraph
    from pydantic.main import BaseModel

    import activities


logger = logging.getLogger(__name__)


@dataclass
class Activity(HasInputOutput, MSONable):
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

    name: str = "Activity"
    jobs: Sequence[Union[Activity, activities.Job]] = field(default_factory=list)
    output_source: Optional[Any] = field(default=None)
    output_schema: Optional[Type[BaseModel]] = None
    config: Dict = field(default_factory=dict)
    uuid: UUID = field(default_factory=uuid4)
    output: Reference = field(init=False)

    def __post_init__(self):
        self.output = Reference(self.uuid, schema=self.output_schema)

    @property
    def input_references(self) -> Tuple[activities.Reference, ...]:
        references = set()
        task_uuids = set()
        for job in self.jobs:
            references.update(job.input_references)
            task_uuids.add(job.uuid)

        return tuple([ref for ref in references if ref.uuid not in task_uuids])

    @property
    def output_references(self) -> Tuple[activities.Reference, ...]:
        from activities.core.reference import find_and_get_references

        if self.output_source is None:
            return tuple()
        return find_and_get_references(self.output_source)

    @property
    def graph(self) -> DiGraph:
        import networkx as nx

        from activities.core.job import store_output

        graph = []
        if self.output_source is not None:
            # only create input-output graph for this activity if it has outputs

            edges = []
            for uuid, refs in self.output_references_grouped.items():
                properties = [
                    ".".join(map(str, ref.attributes)) for ref in refs if ref.attributes
                ]
                properties = properties if len(properties) > 0 else ""
                edges.append((uuid, self.uuid, {"properties": properties}))

            store_output_job = store_output(self.output_source)
            store_output_job.name = self.name + " to store"
            store_output_job.uuid = self.uuid
            store_output_job.metadata["jobs"] = [j.uuid for j in self.jobs]

            graph = nx.DiGraph()
            graph.add_node(
                self.uuid,
                object=store_output_job,
                type="activity",
                label=store_output_job.name,
            )
            graph.add_edges_from(edges)
            graph = [graph]

        job_graphs = [job.graph for job in self.jobs]
        return nx.compose_all(job_graphs + graph)

    def iteractivity(self) -> Generator[Tuple["Activity", Sequence[UUID]], None, None]:
        from activities.core.graph import itergraph

        graph = self.graph
        for node in itergraph(graph):
            parents = [u for u, v in graph.in_edges(node)]
            activity = graph.nodes[node]["object"]
            yield activity, parents

    def set_uuid(self, uuid: UUID):
        self.uuid = uuid
        self.output = self.output.set_uuid(uuid)

    def draw_graph(self):
        from activities.core.graph import draw_graph

        return draw_graph(self.graph)
