from dataclasses import dataclass

from activities import Activity, Outputs, task, initialize_logger
from activities.managers.local import run_activity_locally


# Define encode task and outputs

@dataclass
class EncodeOutputs(Outputs):
    encoded_message: bytes


@task(outputs=EncodeOutputs)
def encode_message(message: str):
    from base64 import b64encode
    encoded_message = b64encode(message.encode())
    return EncodeOutputs(encoded_message=encoded_message)


# Define decode task and outputs

@dataclass
class DecodeOutputs(Outputs):
    decoded_message: str


@task(outputs=DecodeOutputs)
def decode_message(encoded_message: bytes):
    from base64 import b64decode
    decoded_message = b64decode(encoded_message).decode()
    return DecodeOutputs(decoded_message=decoded_message)


# Define encode and decode activities

def get_encode_activity(message: str):
    encode_task = encode_message(message)
    return Activity("Encode", [encode_task], encode_task.outputs)


def get_decode_activity(encoded_message: bytes):
    decode_task = decode_message(encoded_message)
    return Activity("Decode", [decode_task], decode_task.outputs)


# Define an activity of activities
encode = get_encode_activity("Lo, a shadow of horror is risen")
decode = get_decode_activity(encode.outputs.encoded_message)
my_activity = Activity("My activity", [encode, decode], decode.outputs)

# run the activity
initialize_logger()
run_activity_locally(my_activity)
