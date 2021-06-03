"""
In this example, we show case the ability of jobflow to use multiple stores.

Jobs that use additional stores must be run with a JobStore that supports those stores.
"""

from maggma.stores import MemoryStore

from jobflow import JobStore, job, run_locally


@job(data=True)
def generate_big_data():
    """
    Generate some data.

    The data=True in the job decorator tells jobflow to store all outputs in the "data"
    additional store.
    """
    mydata = list(range(1000))
    return mydata


big_data_job = generate_big_data()

# in this example, we use different memory stores for the documents and "data"
# additional store. In practice, any Maggma Store subclass can be used for either store.
docs_store = MemoryStore()
data_store = MemoryStore()
store = JobStore(docs_store, additional_stores={"data": data_store})

# Because our job requires an additional store named "data" we have to use our
# custom store when running the job.
output = run_locally(big_data_job, store=store)

print(output)
