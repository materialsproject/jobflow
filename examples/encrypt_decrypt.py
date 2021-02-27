from dataclasses import dataclass

from activities import Activity
from activities import Outputs
from activities import task
from activities.core.util import initialize_logger
from activities.managers.local import run_activity_locally


# Define encrypt task and outputs

@dataclass
class EncryptOutputs(Outputs):
    encrypted_message: bytes


@task(outputs=EncryptOutputs)
def encrypt_message(message: str):
    from base64 import b64encode
    encrypted_message = b64encode(message.encode())
    return EncryptOutputs(encrypted_message=encrypted_message)


# Define decrypt task and outputs

@dataclass
class DecryptOutputs(Outputs):
    decrypted_message: str


@task(outputs=DecryptOutputs)
def decrypt_message(encrypted_message: bytes):
    from base64 import b64decode
    decrypted_message = b64decode(encrypted_message).decode()
    return DecryptOutputs(decrypted_message=decrypted_message)


# Define encrypt and decrypt activities

def get_encrypt_activity(message: str):
    encrypt_task = encrypt_message(message)
    return Activity("Encrypt", [encrypt_task], encrypt_task.outputs)


def get_decrypt_activity(encrypted_message: bytes):
    decrypt_task = decrypt_message(encrypted_message)
    return Activity("Decrypt", [decrypt_task], decrypt_task.outputs)


# Define an activity of activities
encrypt = get_encrypt_activity("Lo, a shadow of horror is risen")
decrypt = get_decrypt_activity(encrypt.outputs.encrypted_message)
my_activity = Activity("My activity", [encrypt, decrypt], decrypt.outputs)

# run the activity
initialize_logger()
run_activity_locally(my_activity)
