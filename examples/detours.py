from typing import List

from activities import job, Activity, initialize_logger
from activities.core.graph import draw_graph
from activities.managers.local import run_activity_locally


@job
def read_websites():
    from pathlib import Path
    file_contents = Path("websites.txt").read_text()
    return file_contents.split()


@job
def time_website(website: str):
    import urllib.request
    from time import perf_counter

    stream = urllib.request.urlopen(website)
    start_time = perf_counter()
    stream.read()
    end_time = perf_counter()
    stream.close()
    return end_time - start_time


@job
def detour_timing_jobs(websites: List[str]):
    from activities.core.job import Response

    jobs = []
    for website in websites:
        time_job = time_website(website)
        time_job.name = f"time {website}"
        jobs.append(time_job)

    output = [j.output for j in jobs]
    return Response(detour=Activity("timings", jobs, output))


@job
def sum_times(times: List[float]):
    return sum(times)


# create an activity that will first load a list of websites, then generate new
# jobs to calculate the time taken to load each website, and finally, sum all the
# times together

read_websites_job = read_websites()
timings_job = detour_timing_jobs(read_websites_job.output)
sum_job = sum_times(timings_job.output)

my_activity = Activity(jobs=[read_websites_job, timings_job, sum_job])

draw_graph(my_activity.graph).show()

# run the activity
initialize_logger()
responses = run_activity_locally(my_activity)
print(responses)
