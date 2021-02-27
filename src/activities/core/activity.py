"""Define base Activity object."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence, Tuple, Union
from uuid import UUID, uuid4

from maggma.core import Store
from monty.json import MSONable
from networkx import DiGraph

from activities.core.base import HasInputOutput
from activities.core.outputs import Outputs
from activities.core.reference import Reference
from activities.core.task import Task


@dataclass
class Activity(HasInputOutput, MSONable):

    name: str
    tasks: Union[Sequence["Activity"], Sequence[Task]]
    outputs: Optional[Outputs] = None
    host: Optional[UUID] = None
    uuid: UUID = field(default_factory=uuid4)
    config: Dict = field(default_factory=dict)
    output_sources: Optional[Outputs] = field(default=None, init=False)

    def __post_init__(self):
        task_types = set(map(type, self.tasks))
        if len(task_types) != 1:
            raise ValueError(
                "Activity tasks must either be all Task objects or all Activity objects"
            )

        if self.contains_activities:
            for task in self.tasks:
                if task.host is not None:
                    raise ValueError(
                        f"Subactivity {task} already belongs to another activity"
                    )
                task.host = self.uuid

        if self.outputs is not None:
            self.output_sources = self.outputs
            self.outputs = self.outputs.to_reference(self.uuid)

    @property
    def task_type(self) -> str:
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

    def iteractivity(self):
        from activities.core.graph import itergraph

        graph = self.activity_graph
        for node in itergraph(graph):
            parents = [u for u, v in graph.in_edges(node)]
            activity = graph.nodes[node]["object"]

            yield activity, parents

    def run(
        self,
        output_store: Optional[Store] = None,
        output_cache: Optional[Dict[UUID, Dict[str, Any]]] = None,
    ) -> "ActivityResponse":
        # note this only executes the tasks associated with this activity and doesn't
        # run subactivities. If want to excute the full activity tree you should
        # call the run methods of the activities returned by activity.iteractivity()
        if self.contains_activities and self.outputs is None:
            # nothing to do here
            return ActivityResponse()

        output_cache = output_cache or {}
        if self.contains_activities:
            # output sources are from other activities; these should be stored in the
            # output store. Resolve them and store the activity outputs in the DB.
            outputs = self.output_sources.resolve(
                output_store=output_store, output_cache=output_cache
            )
            cache_outputs(self.uuid, outputs, output_cache)

            if output_store:
                outputs.to_db(output_store, self.uuid)

            return ActivityResponse()

        # we have an activity of tasks, run tasks in sequential order
        for i, task in enumerate(self.tasks):  # type: Task
            response = task.run(output_store, output_cache)

            if response.outputs is not None:
                cache_outputs(task.uuid, response.outputs, output_cache)

            if response.store is not None:
                # add the stored data to the activity response
                pass

            if response.exit is not None:
                # need controls for cancelling current activity or full workflow
                pass

            if response.detour is not None:
                # put remaining tasks into new activity; resolve all outputs
                # so far calculated, and add the new activity at the end of the detour
                pass

            if response.restart is not None:
                # cancel remaining tasks, resubmit restart using the same activity
                # id but increment the run index
                # what should we do if response.detour is not None also?
                pass

        outputs = self.output_sources.resolve(
            output_store=output_store, output_cache=output_cache
        )

        if output_store:
            outputs.to_db(output_store, self.uuid)

        return ActivityResponse()


def cache_outputs(uuid: UUID, outputs: Outputs, cache: Dict[UUID, Dict[str, Any]]):
    for name, output in outputs.items():
        if uuid not in cache:
            cache[uuid] = {}

        cache[uuid][name] = output


@dataclass
class ActivityResponse:
    # TODO: Consider merging this with TaskResponse

    detour: Optional[Activity] = None
    restart: Optional[Activity] = None
    store: Optional[Dict[str, Any]] = None
    exit: bool = False
