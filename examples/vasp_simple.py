from dataclasses import dataclass

from activities.activity import Activity
from activities.maker import Maker
from activities.outputs import Outputs


@dataclass
class VaspOutputs(Outputs):
    dirname: str

    @staticmethod
    def from_directory(self):
        pass


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
class RelaxMaker(Maker):

    name: str = "relax"
    vasp_input_set = "MPRelaxSet"

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
