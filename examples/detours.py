from dataclasses import dataclass
from typing import List

from activities import Outputs, task, Activity, initialize_logger, Detour
from activities.managers.local import run_activity_locally


# define task and outputs to generate letters of the alphabet

@dataclass
class AlphabetOutputs(Outputs):
    letters: List[str]


@task(outputs=AlphabetOutputs)
def generate_alphabet():
    import string
    return AlphabetOutputs(list(string.ascii_lowercase))


# define task, outputs and, activity to repeat a letter 5 times

@dataclass
class RepeatOutputs(Outputs):
    repeated_letters: str


@task(outputs=RepeatOutputs)
def repeat_letter(letter: str):
    return RepeatOutputs(letter * 5)


def get_repeat_activity(letter: str):
    repeat_letter_task = repeat_letter(letter)
    return Activity("Repeat", [repeat_letter_task], repeat_letter_task.outputs)


# define a task to generate repeat activities for multiple letters

@task(outputs=AlphabetOutputs)
def repeat_letters(letters: List[str]):
    activities = [get_repeat_activity(letter) for letter in letters]
    outputs = AlphabetOutputs([a.outputs.repeated_letters for a in activities])
    detour_activity = Activity("Repeats", activities, outputs)
    return Detour(detour_activity)


# create an activity that will generate the letters of the alphabet, then submit
# a new activity for each letter to repeat it 5 times.
alphabet = generate_alphabet()
repeat = repeat_letters(letters=alphabet.outputs.letters)
my_activity = Activity("Repeat alphabet", [alphabet, repeat], repeat.outputs)

# run the activity
initialize_logger()
run_activity_locally(my_activity)
