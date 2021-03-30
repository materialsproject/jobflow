from activities import Activity, job
from activities.core.graph import draw_graph
from activities.managers.local import run_activity_locally


@job
def encode_message(message: str):
    from base64 import b64encode
    return b64encode(message.encode()).decode()


@job
def decode_message(message: str):
    from base64 import b64decode
    return b64decode(message.encode()).decode()


encode = encode_message("Lo, a shadow of horror is risen")
decode = decode_message(encode.output)
my_activity = Activity(jobs=[encode, decode])

draw_graph(my_activity.graph).show()

# responses contains the output of all jobs
responses = run_activity_locally(my_activity)
print(responses)
