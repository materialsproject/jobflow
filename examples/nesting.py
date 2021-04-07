import random
from activities import job, Activity, run_locally


@job
def generate_first_name():
    return random.choice(["Alex", "Ryan", "Liam", "Katie", "Lucy", "Hillary"])


@job
def generate_second_name():
    return random.choice(["Jones", "Jackson", "Jamieson", "Jacobs", "Jagger"])


@job
def connect_name(first_name, second_name):
    return f"{first_name} {second_name}"


@job
def print_inputs(inputs):
    print(inputs)


def get_name_activity():
    first_name = generate_first_name()
    second_name = generate_second_name()
    full_name = connect_name(first_name.output, second_name.output)
    return Activity(
        [first_name, second_name, full_name], full_name.output, name="Get Name"
    )


name_activity_a = get_name_activity()
name_activity_b = get_name_activity()
print_job = print_inputs([name_activity_a.output, name_activity_b.output])

# create an activity to contain the nested activities
outer_activity = Activity([name_activity_a, name_activity_b, print_job])

# draw the activity graph
outer_activity.draw_graph().show()

# run the activity
run_locally(outer_activity)
