"""Define base Activity object."""
import logging
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Union
from uuid import UUID, uuid4

from maggma.core import Store
from monty.json import MSONable
from networkx import DiGraph

from activities.core.base import HasInputOutput
from activities.core.outputs import Outputs
from activities.core.reference import Reference
from activities.core.task import Task

logger = logging.getLogger(__name__)


@dataclass
class Activity(HasInputOutput, MSONable):

    name: str
    tasks: Union[Sequence["Activity"], Sequence[Task]]
    outputs: Optional[Outputs] = None
    host: Optional[UUID] = None
    uuid: UUID = field(default_factory=uuid4)
    config: Dict = field(default_factory=dict)
    output_sources: Optional[Outputs] = field(default=None)

    def __post_init__(self):
        task_types = set(map(type, self.tasks))
        if len(task_types) > 1:
            raise ValueError(
                "Activity tasks must either be all Task objects or all Activity objects"
            )

        if self.contains_activities:
            for task in self.tasks:
                if task.host is not None and task.host != self.uuid:
                    print(task.host, self.uuid)
                    raise ValueError(
                        f"Subactivity {task} already belongs to another activity"
                    )
                task.host = self.uuid

        if self.outputs is None and self.output_sources is not None:
            self.outputs = self.outputs_sources.to_reference(self.uuid)
        elif self.outputs is not None and self.output_sources is None:
            self.output_sources = self.outputs
            self.outputs = self.outputs.to_reference(self.uuid)

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
    def input_references(self) -> Tuple[Reference, ...]:
        references = set()
        task_uuids = set()
        for task in self.tasks:
            references.update(task.input_references)
            task_uuids.add(task.uuid)

        return tuple([ref for ref in references if ref.uuid not in task_uuids])

    @property
    def output_references(self) -> Tuple[Reference, ...]:
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

    def run(
        self,
        output_store: Optional[Store] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
    ) -> "ActivityResponse":
        logger.info(f"Starting activity - {self.name} ({self.uuid})")

        # note this only executes the tasks associated with this activity and doesn't
        # run subactivities. If want to excute the full activity tree you should
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
                outputs.to_db(output_store, self.uuid)

            logger.info(f"Finished activity - {self.name} ({self.uuid})")
            return ActivityResponse()

        activity_response = ActivityResponse()

        # we have an activity of tasks, run tasks in sequential order
        for i, task in enumerate(self.tasks):  # type: int, Task
            response = task.run(output_store, output_cache)

            if response.store is not None:
                # add the stored data to the activity response
                activity_response.store.update(response.store)

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

            if response.outputs is not None:
                cache_outputs(task.uuid, response.outputs, output_cache)

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

            if output_store:
                outputs.to_db(output_store, self.uuid)

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


def cache_outputs(uuid: UUID, outputs: Outputs, cache: Dict[UUID, Dict[str, Any]]):
    for name, output in outputs.items():
        if uuid not in cache:
            cache[uuid] = {}

        cache[uuid][name] = output


@dataclass
class ActivityResponse:
    # TODO: Consider merging this with TaskResponse

    detour: Optional[Tuple[Activity, Activity]] = None
    restart: Optional[Activity] = None
    store: Dict[str, Any] = field(default_factory=dict)
    stop_children: bool = False
    stop_activities: bool = False
