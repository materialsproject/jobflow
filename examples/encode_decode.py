from flows import Flow, job, run_locally


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
act = Flow([encode, decode])

# draw the flow graph
act.draw_graph().show()

# run the flow, "output" contains the output of all jobs
output = run_locally(act)
print(output)
