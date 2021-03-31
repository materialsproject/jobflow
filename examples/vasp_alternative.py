from dataclasses import dataclass, field
from typing import Optional

from activities import Activity, Maker, Outputs, task

# define outputs class for VASP calculations


@dataclass
class VaspOutputs(Outputs):
    dirname: str
    dielectric_constant: Optional[list] = None

    @staticmethod
    def from_directory(self):
        pass


# define functions that are used in the job


def copy_vasp_files():
    pass


def run_vasp():
    pass


def write_vasp_input_set():
    pass


# there are two ways to write a job. The first is by decorating a function


@activity
def get_relax(
    structure,
    prev_vasp_dir=None,
    vasp_input_set="MPStaticSet",
    copy_vasp_kwargs=None,
    run_vasp_kwargs=None,
):
    copy_vasp_kwargs = copy_vasp_kwargs or {}
    run_vasp_kwargs = run_vasp_kwargs or {}

    from_prev = False
    if prev_vasp_dir is not None:
        copy_vasp_files(prev_vasp_dir, **copy_vasp_kwargs)
        from_prev = True

    write_vasp_input_set(structure, vasp_input_set, from_prev=from_prev)
    run_vasp(**run_vasp_kwargs)
    outputs = VaspOutputs.from_directory()
    return outputs


relax = get_relax(structure)


# The second way is by decorating the function of a class


@dataclass
class RelaxMaker(Maker):

    name: str = "relax"
    vasp_input_set: str = "MPRelaxSet"
    vasp_input_set_kwargs: dict = field(default_factory=dict)
    copy_vasp_kwargs: dict = field(default_factory=dict)
    run_vasp_kwargs: dict = field(default_factory=dict)

    @activity
    def make(self, structure, prev_vasp_dir=None) -> VaspOutputs:
        from_prev = False
        if prev_vasp_dir is not None:
            copy_vasp_files(prev_vasp_dir, **self.copy_vasp_kwargs)
            from_prev = True

        write_vasp_input_set(
            structure,
            self.vasp_input_set,
            self.vasp_input_set_kwargs,
            from_prev=from_prev,
        )
        run_vasp(**self.run_vasp_kwargs)
        outputs = VaspOutputs.from_directory()
        return outputs


# we will use this second way in atomate2.
relax = RelaxMaker().make(structure)


# when submitting the calculations you need to construct and activity object
# in this example, these tasks can run in parallel
relax1 = RelaxMaker().make(structure)
relax2 = RelaxMaker().make(structure)
activity = ActivitySet([relax1, relax2])

# in this example, the tasks are chained and will run in serial
relax1 = RelaxMaker().make(structure)
relax2 = RelaxMaker().make(relax1.output.structure)
activity = Activity(tasks=[relax1, relax2])

# editing the kwargs of an activity should be easy
modify_incar_kwargs(activity, {"NSW": 1000})

# Activities are collections of tasks/other activities. For example,
# see the following example for a band structure calculation with optional
# line mode and uniform band calculations


@dataclass
class BandStructureMaker:
    name: str = "band structure"
    static_maker: Maker = field(default_factory=StaticMaker)
    nscf_line_maker: Optional[Maker] = field(default_factory=NonScfLineMaker)
    nscf_uniform_maker: Optional[Maker] = field(default_factory=NonScfUniformMaker)

    def make(self, structure, prev_vasp_dir=None):
        static = self.static_maker.make(structure, prev_vasp_dir=prev_vasp_dir)
        tasks = [static]

        outputs = BandStructureOutputs(structure=static.structure)
        if self.nscf_line_maker:
            nscf_line = self.nscf_line_maker.make(
                structure, prev_vasp_dir=static.prev_vasp_dir
            )
            outputs.line_mode_bandstructure = nscf_line.line_mode_bandstructure
            outputs.line_mode_task = nscf_line
            tasks.append(nscf_line)

        if self.nscf_uniform_maker:
            nscf_uniform = self.nscf_uniform_maker.make(
                structure, prev_vasp_dir=static.prev_vasp_dir
            )
            outputs.uniform_bandstructure = nscf_uniform.uniform_bandstructure
            outputs.uniform_task = nscf_uniform
            tasks.append(nscf_uniform)

        return Activity("band structure", tasks, outputs)


# In this new approach, tasks and activities can be combined into a new activity
# I.e., relax is a job and band_structure is an activity of tasks.

relax = RelaxMaker().make(structure)
band_structure = BandStructureMaker().make(relax.structure)
activity = Activity(tasks=[relax, band_structure])

# to change the static calculation settings oor nscf line settings then you just
# supply a different maker class to BandStructureMaker, e.g.

static_maker = StaticMaker(vasp_input_set="MITInputSet")
band_structure_maker = BandStructureMaker(static_maker=static_maker)
band_structure = BandStructureMaker().make(relax.structure)

# Note the above modification should also be possible using something like
modify_input_set(activity, "MITInputSet", maker_filter=StaticMaker)

# Or the more general
activity.update_kwargs({"vasp_input_set": "MITInputSet"}, maker_filter=StaticMaker)


# we could have abstract VaspMaker classes for Makers that produce tasks to standardize
# the class arguments (i.e, that force vasp_input_set, etc to be present).

# If you wanted to create a re-usable activity for a relax and band structure
# this then becomes easy, it is literally just the same code as above.
# (this is a silly example but you get the idea)


@dataclass
class RelaxBandStructureMaker:
    name: str = "band structure"
    relax_maker: Maker = field(default_factory=RelaxMaker)
    band_structure_maker: Maker = field(default_factory=BandStructureMaker)

    def make(self, structure, prev_vasp_dir=None):
        relax = self.relax_maker.make(structure, prev_vasp_dir=prev_vasp_dir)
        band_structure = self.band_structure_maker.make(relax.structure)
        return Activity(
            "relax & band structure", [relax, band_structure], band_structure
        )


# having everything in one function reduces the need for passing things through the
# fw spec (a major source of complexity in atomate and something that is very
# difficult to debug locally and is often undocumented. Take a look at TransferNEBTask.)


@dataclass
class ConvergeMaker(Maker):

    name: str = "converge"

    @task(outputs=VaspOutputs)
    def make(self, structure):
        for i in range(100, 500, 50):
            write_vasp_input_set(structure)
            run_vasp(**self.run_vasp_kwargs)

        return
