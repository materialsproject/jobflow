"""A demonstration of sequential jobs with missing references."""

from jobflow import Flow, OnMissing, job, run_locally


@job
def func(fail: bool = False):
    """Failable function."""
    if fail:
        raise ValueError("An error occurred.")
    return 1


@job
def collect(job_outputs):
    """Job that allows some parents to fail."""
    total = 0
    for jo in job_outputs:
        if jo is None:
            continue
        total += jo
    if total < 1:
        raise ValueError("No enough finished parents.")
    return total


job1, job2, job3 = func(), func(), func(fail=True)
job_outputs = [job1.output, job2.output, job3.output]
collect_job = collect(job_outputs)
collect_job.config.on_missing_references = OnMissing.NONE
flow = Flow([job1, job2, job3, collect_job])

# run the flow, you can
res = run_locally(flow)
n_finished = 2
assert res[collect_job.uuid][1].output == n_finished
