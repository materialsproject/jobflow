"""Define base Activity object."""
from __future__ import annotations

import logging
import typing
from dataclasses import dataclass, field
from uuid import uuid4

from monty.json import MSONable

from activities.core.base import HasInputOutput

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Generator, Optional, Sequence, Tuple, Union
    from uuid import UUID

    from maggma.core import Store
    from networkx import DiGraph

    import activities


logger = logging.getLogger(__name__)


@dataclass
class Activity(HasInputOutput, MSONable):
    """
    An Activity contains a sequence of Tasks or other Activities to execute.

    The :obj:`Activity: object is the main tool for constructing workflows. Activities
    can either contain tasks or other activities but not a mixture of both.
    Like :obj:`Task` objects, activities can also have outputs. The outputs
    of an activity will likely be stored in a database (depending on the manager used
    to run the activity), whereas the outputs of tasks are only available while the
    activity is running.

    .. Note::
        There is one important difference between activities containing :obj:`Task`
        objects and those containing other :obj:`Activity` objects: Activities
        containing :obj:`Task` objects will execute the tasks in the order they are
        given in the ``tasks`` list, whereas activities containing :obj:`Activity`
        objects sorted to determine the optimal execution order.

        This may be changed in a future release.

    Parameters
    ----------
    name
        The activity name.
    tasks
        The tasks to be run. Can either be a list of :obj:`Task` objects or a list of
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
    output_sources
        The sources of the output of the activity. Set automatically.

    Examples
    --------

    Below we define a simple task to add two numbers, and create an activity containing
    that task.

    >>> from activities import task, Activity
    ...
    >>> @task
    ... def add(a, b):
    ...     return a + b
    ...
    >>> add_task = add(1, 2)
    >>> activity = Activity(tasks=[add_task])

    If we were to run this activity, what would happen to the output of the task? It
    would be lost as the outputs of the activity was not defined. To remedy that, we
    can set the outputs of the activity to be the outputs of the ``add_task`` task.

    >>> activity = Activity(tasks=[add_task], outputs=add_task.outputs)

    If we run the activity, we get an :obj:`ActivityResponse` object, that contains the
    outputs among other things.

    >>> response = activity.run()
    >>> response.outputs
    Value(value=3)

    It is not recommended to run the activity directly as we have done above. Instead,
    we provide several activity managers for running activities locally or remotely.

    >>> from activities.managers.local import run_activity_locally
    >>> response = run_activity_locally(activity)

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
    ValueError("Cannot mix Activity objects and Task objects in the same Activity")

    By defining the task output class, activities can make use of static
    parameter checking to ensure that connections between tasks are valid.

    >>> from activities.core.outputs import Number
    >>> @task(outputs=Number)
    ... def add(a, b):
    ...     return Number(a + b)
    ...
    ... task1 = add(1, 2)
    ... task2 = add(task1.outputs.bad_output, 5)
    AttributeError: 'Number' object has no attribute 'bad_output'
    """

    name: str = "Activity"
    tasks: Union[Sequence[Activity], Sequence[activities.Task]] = field(
        default_factory=list
    )
    outputs: Optional[activities.Outputs] = None
    config: Dict = field(default_factory=dict)
    host: Optional[UUID] = None
    uuid: UUID = field(default_factory=uuid4)
    output_sources: Optional[activities.Outputs] = field(default=None)

    def __post_init__(self):
        from activities import Outputs
        from activities.core.outputs import Dynamic

        task_types = set(map(type, self.tasks))
        if len(task_types) > 1:
            raise ValueError(
                "Cannot mix Activity objects and Task objects in the same Activity"
            )

        if self.contains_activities:
            for task in self.tasks:
                if task.host is not None and task.host != self.uuid:
                    raise ValueError(
                        f"Subactivity {task} already belongs to another activity"
                    )
                task.host = self.uuid

        if self.outputs is not None and self.output_sources is None:
            if isinstance(self.outputs, dict):
                self.outputs = Dynamic(**self.outputs)
            elif not isinstance(self.outputs, Outputs):
                self.outputs = Dynamic(value=self.outputs)

            self.output_sources = self.outputs
            self.outputs = self.outputs.fields_to_references(uuid=self.uuid)

    @property
    def task_type(self) -> str:
        if len(self.tasks) < 1:
            # this is just a ToOutputs container task, probably from a detour.
            return "task"
        else:
            return "activity" if isinstance(self.tasks[0], Activity) else "task"

    @property
    def contains_activities(self) -> bool:
        return self.task_type == "activity"

    @property
    def input_references(self) -> Tuple[activities.Reference, ...]:
        references = set()
        task_uuids = set()
        for task in self.tasks:
            references.update(task.input_references)
            task_uuids.add(task.uuid)

        return tuple([ref for ref in references if ref.uuid not in task_uuids])

    @property
    def output_references(self) -> Tuple[activities.Reference, ...]:
        if self.output_sources is None:
            return tuple()
        return self.output_sources.references

    @property
    def activity_graph(self) -> DiGraph:
        import networkx as nx

        from activities.core.graph import activity_input_graph, activity_output_graph

        if self.contains_activities:
            graph = activity_output_graph(self)
            activity_graphs = [task.activity_graph for task in self.tasks]
            return nx.compose_all(activity_graphs + [graph])
        else:
            return activity_input_graph(self)

    @property
    def task_graph(self) -> DiGraph:
        import networkx as nx

        from activities.core.graph import activity_output_graph, task_graph

        if self.contains_activities:
            graph = activity_output_graph(self)
            activity_graphs = [activity.task_graph for activity in self.tasks]
            return nx.compose_all(activity_graphs + [graph])
        else:
            return task_graph(self)

    def iteractivity(self) -> Generator[Tuple["Activity", Sequence[UUID]], None, None]:
        from activities.core.graph import itergraph

        graph = self.activity_graph
        for node in itergraph(graph):
            parents = [u for u, v in graph.in_edges(node)]
            activity = graph.nodes[node]["object"]
            yield activity, parents

    def set_uuid(self, uuid: UUID):
        self.uuid = uuid
        if self.contains_activities:
            for task in self.tasks:
                task.host = self.uuid
        if self.outputs:
            self.outputs = self.outputs.fields_to_references(uuid=uuid)

    def run(
        self,
        output_store: Optional[Store] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
    ) -> "ActivityResponse":
        logger.info(f"Starting activity - {self.name} ({self.uuid})")

        # note this only executes the tasks associated with this activity and doesn't
        # run subactivities. If want to execute the full activity tree you should
        # call the run methods of the activities returned by activity.iteractivity()
        if self.contains_activities and self.outputs is None:
            logger.info(f"Activity has no outputs and no tasks, skipping...")
            # nothing to do here
            return ActivityResponse()

        output_cache = output_cache if output_cache is not None else {}
        if self.contains_activities:
            # output sources are from other activities; these should be stored in the
            # output store. Resolve them and store the activity outputs in the DB.
            outputs = self.output_sources.resolve(
                output_store=output_store, output_cache=output_cache
            )
            cache_outputs(self.uuid, outputs, output_cache)

            if output_store:
                outputs.to_store(output_store, self.uuid)

            logger.info(f"Finished activity - {self.name} ({self.uuid})")
            return ActivityResponse(outputs=outputs)

        activity_response = ActivityResponse()

        # we have an activity of tasks, run tasks in sequential order
        for i, task in enumerate(self.tasks):  # type: int, activities.Task
            response = task.run(output_store, output_cache)

            if response.store is not None:
                # add the stored data to the activity response
                activity_response.store.update(response.store)

            if response.outputs is not None:
                cache_outputs(task.uuid, response.outputs, output_cache)

            if response.detour is not None:
                # put remaining tasks into new activity; resolve all outputs
                # so far calculated, and add the new activity at the end of the detour
                activity_response.detour = create_detour_activities(
                    task.uuid,
                    i,
                    response.detour,
                    self,
                    output_store=output_store,
                    output_cache=output_cache,
                )
                break

            if response.restart is not None:
                # cancel remaining tasks, resubmit restart using the same activity
                # id but increment the run index
                # what should we do if response.detour is not None also?
                pass

            if response.stop_children:
                activity_response.stop_children = True

            if response.stop_activities:
                activity_response.stop_activities = True

            if response.stop_tasks:
                logging.warning(
                    "Stopping subsequent tasks. This may break output references."
                )
                break

        if self.output_sources and not activity_response.detour:
            outputs = self.output_sources.resolve(
                output_store=output_store, output_cache=output_cache
            )
            cache_outputs(self.uuid, outputs, output_cache)
            activity_response.outputs = outputs

            if output_store:
                outputs.to_store(output_store, self.uuid)

        logger.info(f"Finished activity - {self.name} ({self.uuid})")
        return activity_response


def create_detour_activities(
    task_uuid: UUID,
    task_index: int,
    detour_activity: Activity,
    original_activity: Activity,
    output_store: Optional[Store] = None,
    output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
):
    # put remaining tasks into new activity; resolve all outputs
    # so far calculated, and add the new activity at the end of the detour
    # when detouring we have to make 2 new activities:
    # 1. The actual detoured activity as returned by the Task but with the
    #    UUID set to the UUID of the task that generated it (so that its references)
    #    can be resolved.
    # 2. An activity with the same UUID as the generating Activity, with any remaining
    #    tasks, and the same outputs. For this activity, some of the outputs may already
    #    have been calculated, se we should resolve them now, as it won't be possible
    #    to resolve them at a later stage as they are task outputs not activity outputs
    #    (and therefore won't have been added to the activity_outputs collection).
    detour_activity.set_uuid(task_uuid)
    detour_activity.host = original_activity.uuid
    original_activity.output_sources.resolve(
        output_store=output_store, output_cache=output_cache, error_on_missing=False
    )
    remaining_tasks = Activity(
        name=original_activity.name,
        tasks=original_activity.tasks[task_index + 1 :],
        outputs=original_activity.outputs,
        output_sources=original_activity.output_sources,
        config=original_activity.config,
        host=original_activity.host,
        uuid=original_activity.uuid,
    )
    return detour_activity, remaining_tasks


def cache_outputs(
    uuid: UUID, outputs: activities.Outputs, cache: Dict[UUID, Dict[str, Any]]
):
    for name, output in outputs.items():
        if uuid not in cache:
            cache[uuid] = {}

        cache[uuid][name] = output


@dataclass
class ActivityResponse:
    # TODO: Consider merging this with TaskResponse

    outputs: Optional[activities.Outputs] = None
    detour: Optional[Tuple[Activity, Activity]] = None
    restart: Optional[Activity] = None
    store: Dict[str, Any] = field(default_factory=dict)
    stop_children: bool = False
    stop_activities: bool = False
