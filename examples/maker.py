from dataclasses import dataclass

from jobflow import Flow, Maker, job, run_locally


@dataclass
class AddMaker(Maker):
    name: str = "add"
    c: int = 10

    @job
    def make(self, a, b, d=0.5):
        return a + b + self.c + d


maker = AddMaker(c=10)

add_first = maker.make(1, 2)
add_second = maker.make(add_first.output, 5)
flow = Flow([add_first, add_second])

flow.update_maker_kwargs({"_inc": {"c": 50}}, dict_mod=True)
flow.update_kwargs({"d": 0.2})

# run the flow, "output" contains the output of all jobs
output = run_locally(flow)
print(output)
