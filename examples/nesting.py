import random

from jobflow import Flow, job, run_locally


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


def get_name_flow():
    first_name = generate_first_name()
    second_name = generate_second_name()
    full_name = connect_name(first_name.output, second_name.output)
    return Flow([first_name, second_name, full_name], full_name.output, name="Get Name")


name_flow_a = get_name_flow()
name_flow_b = get_name_flow()
print_job = print_inputs([name_flow_a.output, name_flow_b.output])

# create a new flow to contain the nested flow
outer_flow = Flow([name_flow_a, name_flow_b, print_job])

# draw the flow graph
outer_flow.draw_graph().show()

# run the flow
run_locally(outer_flow)
