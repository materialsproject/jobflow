"""A demonstration of how to enforce linear ordering of jobs."""
from jobflow import Flow, JobOrder, job


@job
def add(a, b):
    return a + b


# No edges between the jobs as they do not depend on each other
add_job_first = add(1, 2)
add_job_second = add(4, 6)
auto_flow = Flow([add_job_first, add_job_second], order=JobOrder.AUTO)
auto_flow.draw_graph().show()

# flow graph now shows an edge between the jobs due to the linear execution order
add_job_first = add(1, 2)
add_job_second = add(4, 6)
linear_flow = Flow([add_job_first, add_job_second], order=JobOrder.LINEAR)
linear_flow.draw_graph().show()
