from activities import Activity, JobOrder, job


@job
def add(a, b):
    return a + b


add_job_first = add(1, 2)
add_job_second = add(4, 6)

my_activity = Activity(jobs=[add_job_first, add_job_second], order=JobOrder.LINEAR)

# activity graph shows an edge between the jobs even though there are no shared outputs
my_activity.draw_graph().show()
