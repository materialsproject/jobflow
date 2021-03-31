from activities import Activity, JobOrder, job


@job
def add(a, b):
    return a + b


add_job_first = add(1, 2)
add_job_second = add(4, 6)

# No edges between the tasks as the tasks do not depend on each other
auto_activity = Activity(jobs=[add_job_first, add_job_second], order=JobOrder.AUTO)
auto_activity.draw_graph().show()

# activity graph now shows an edge between the jobs due to the linear execution order
linear_activity = Activity(jobs=[add_job_first, add_job_second], order=JobOrder.LINEAR)
linear_activity.draw_graph().show()
