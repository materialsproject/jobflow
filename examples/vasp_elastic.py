from dataclasses import dataclass, field
from typing import Any, Dict, List

from activities.activity import Activity
from activities.maker import Maker
from activities.outputs import Outputs


@dataclass
class VaspOutputs(Outputs):
    dirname: str

    @staticmethod
    def from_directory(self):
        pass


@dataclass
class PerturbationOutputs(Outputs):
    perturbed_structures: List["Structure"]
    perturbed_forces: List["Matrix"]


@task
def copy_vasp_files():
    pass


@task
def vasp_to_db():
    pass


@task
def run_vasp_custodian():
    pass


@task
def write_vasp_input_set():
    pass


@task(outputs=VaspOutputs)
def vasp_to_outputs():
    return VaspOutputs.from_directory()


@dataclass
class StaticMaker(Maker):

    name: str = "relax"
    vasp_input_set: str = "MPRelaxSet"

    def get_activity(self, structure, prev_vasp_dir=None):

        tasks = []
        from_prev = False
        if prev_vasp_dir is not None:
            tasks.append(copy_vasp_files(prev_vasp_dir))
            from_prev = True

        write_inputs = write_vasp_input_set(
            structure, self.vasp_input_set, from_prev=from_prev
        )
        run = run_vasp_custodian()
        to_db = vasp_to_db(additional_fields={"task_label": self.name})
        to_outputs = vasp_to_outputs()

        tasks += [write_inputs, run, to_db, to_outputs]

        return Activity(self.name, tasks, to_outputs.outputs)


@task(outputs=StructureSet)
def perturb_structure(structure):
    perturbed_structures = get_perturbed_structure(structure)
    return StructureSet(structures=perturbed_structures)


@task(outputs=OutputSet(VaspOutputs))
def detour_activities(structures, activity_maker):
    from activities.core.task import Detour

    tasks = [activity_maker.get_activity(s) for s in structures]
    outputs = OutputSet(outputs=[calc.outputs for calc in calcs])
    activity = Activity("Perturbed statics", tasks, outputs)
    return Detour(activity)


@dataclass
class PerturbationsMaker(Maker):

    name: str = "Perturbation"
    pertubations_kwargs: Dict[str, Any] = field(default_factory=dict)
    static_maker: Maker = field(default_factory=StaticMaker)

    def get_activity(self, structure) -> Activity:
        structures = perturb_structure(structure, **self.pertubations_kwargs)
        statics = detour_activities(structures.outputs.structures, self.static_maker)
        outputs = PerturbationOutputs(
            perturbed_forces=statics.outputs.all("forces"),
            perturbed_structures=statics.outputs.all("structure"),
        )
        return Activity(self.name, [structures, statics], outputs)


static_maker = StaticMaker(vasp_input_set="UCLStaticSet")
pertubations = PerturbationsMaker(static_maker=static_maker).get_activity(structure)


@dataclass
class ElasticMaker(Maker):

    name: str = "Perturbation"
    pertubations_kwargs: Dict[str, Any] = field(default_factory=dict)
    static_maker: Maker = field(default_factory=StaticMaker)

    def get_activity(self, structure) -> Activity:
        # this will start off as 3 tasks
        # but end up as three activities
        structures = perturb_structure(structure, **self.pertubations_kwargs)
        statics = detour_activities(structures.outputs.structures, self.static_maker)
        elastic = calculate_elastic_tensor(
            perturbed_forces=statics.outputs.all("forces"),
            perturbed_structures=statics.outputs.all("structure"),
        )
        return Activity(self.name, [structures, statics], elastic.outputs)
