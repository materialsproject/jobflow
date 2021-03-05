from activities import Activity


def test_serialization():
    import json
    from uuid import uuid4

    from monty.json import MontyDecoder, MontyEncoder

    activity = Activity("MyActivity", [])
    activity_host = Activity("MyActivity", [activity])
    host_uuid = activity_host.uuid

    encoded_activity = json.loads(MontyEncoder().encode(activity_host))
    decoded_activity = MontyDecoder().process_decoded(encoded_activity)

    assert decoded_activity.tasks[0].host == host_uuid
