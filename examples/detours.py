from dataclasses import dataclass
from typing import List

from activities import Outputs, task, Activity, initialize_logger, Detour
from activities.managers.local import run_activity_locally


# define task and outputs to generate letters of the alphabet

@dataclass
class ListOfStrings(Outputs):
    strings: List[str]


@task(outputs=ListOfStrings)
def generate_alphabet():
    import string
    return ListOfStrings(list(string.ascii_lowercase))


# define task, outputs and, activity to repeat a string 5 times

@dataclass
class String(Outputs):
    string: str


@task(outputs=String)
def repeat_string(string: str):
    return String(string * 5)


def get_repeat_activity(string: str):
    repeat_string_task = repeat_string(string)
    return Activity("Repeat", [repeat_string_task], repeat_string_task.outputs)


# define a task to generate repeat activities for multiple strings

@task(outputs=ListOfStrings)
def repeat_strings(strings: List[str]):
    activities = [get_repeat_activity(string) for string in strings]
    outputs = ListOfStrings([activity.outputs.string for activity in activities])
    detour_activity = Activity("Repeats", activities, outputs)
    return Detour(detour_activity)


# create an activity that will generate the letters of the alphabet, then submit
# a new activity for each letter to repeat it 5 times.
alphabet = generate_alphabet()
repeat = repeat_strings(strings=alphabet.outputs.strings)
my_activity = Activity("Repeat alphabet", [alphabet, repeat], repeat.outputs)

# run the activity
initialize_logger()
run_activity_locally(my_activity)
