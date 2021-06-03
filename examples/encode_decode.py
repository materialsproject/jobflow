"""A simple example to show message passing between jobs."""
from jobflow import Flow, job, run_locally


@job
def encode_message(message):
    """Encode a message using base64."""
    from base64 import b64encode

    return b64encode(message.encode()).decode()


@job
def decode_message(message):
    """Decode a message from base64."""
    from base64 import b64decode

    return b64decode(message.encode()).decode()


# Create two jobs, the first to encode a message and the second to decode it.
encode = encode_message("Lo, a shadow of horror is risen")
decode = decode_message(encode.output)

# Create a flow containing the jobs. The order of the jobs doesn't matter and will be
# determined by the connectivity of the jobs.
flow = Flow([encode, decode])

# draw the flow graph
flow.draw_graph().show()

# run the flow, "output" contains the output of all jobs
output = run_locally(flow)
print(output)
