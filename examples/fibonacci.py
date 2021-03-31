from activities import Response, job, run_locally


@job
def fibonacci(smaller: int, larger: int, stop_point: int = 1000):
    total = smaller + larger

    if total > stop_point:
        return total

    new_job = fibonacci(larger, total, stop_point=stop_point)
    return Response(output=total, restart=new_job)


fibonacci_job = fibonacci(1, 1)

# run the job; responses will contain the output from all jobs
responses = run_locally(fibonacci_job)
print(responses)
