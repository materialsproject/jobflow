from jobflow import Flow, job, run_locally


@job(data=True)
def generate_big_data():
    data = list(range(1000))
    return data


@job
def calculate_sum(numbers):
    return sum(numbers)


big_data_job = generate_big_data()
sum_job = calculate_sum(big_data_job.output)
flow = Flow([big_data_job, sum_job])

output = run_locally(flow)
print(output)
