from dataclasses import dataclass

from activities import Maker, job, Activity, run_locally


@dataclass
class AddMaker(Maker):
    name: str = "Add"
    c: int = 10

    @job
    def make(self, a, b, d=0.5):
        return a + b + self.c + d


maker = AddMaker(c=10)

add_first = maker.make(1, 2)
add_second = maker.make(add_first.output, 5)

my_activity = Activity(jobs=[add_first, add_second])

my_activity.update_maker_kwargs({"_inc": {"c": 50}}, dict_mod=True)
my_activity.update_kwargs({"d": 0.2})

# run the activity, "responses" contains the output of all jobs
responses = run_locally(my_activity)
print(responses)
