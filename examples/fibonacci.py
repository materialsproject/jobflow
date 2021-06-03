"""A dynamic workflow that calculates the Fibonacci sequence."""
from jobflow import Response, job, run_locally


@job
def fibonacci(smaller, larger, stop_point=1000):
    """Calculate the next number in the Fibonacci sequence.

    If the number is larger than stop_point, the job will stop the workflow
    execution, otherwise, a new job will be submitted to calculate the next number.
    """
    total = smaller + larger

    if total > stop_point:
        return total

    new_job = fibonacci(larger, total, stop_point=stop_point)
    return Response(output=total, addition=new_job)


fibonacci_job = fibonacci(1, 1)

# run the job; responses will contain the output from all jobs
responses = run_locally(fibonacci_job)
print(responses)
