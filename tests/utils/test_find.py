def test_find_key():
    from monty.json import MSONable, jsanitize

    from jobflow.utils import find_key

    data = {"a": [0, {"b": 1, "x": 3}], "c": {"d": {"x": 3}}}
    result = find_key(data, "x")
    assert len(result) == 2
    assert ["c", "d"] in result
    assert ["a", 1] in result

    result = find_key(data, "x", include_end=True)
    assert len(result) == 2
    assert ["c", "d", "x"] in result
    assert ["a", 1, "x"] in result

    data = {"a": {"x": {"x": 1}}, "b": {"x": 0}}
    result = find_key(data, "x", nested=False)
    assert len(result) == 2
    assert ["a"] in result
    assert ["b"] in result

    result = find_key(data, "x", nested=True)
    assert len(result) == 3
    assert ["a", "x"] in result
    assert ["a"] in result
    assert ["b"] in result

    class MyObj(MSONable):
        def __init__(self, a):
            self.a = a

    data = {"a": [0, {"b": 1, "x": 3}], "c": {"d": {"x": MyObj(a=1)}}}
    data = jsanitize(data, strict=True)
    assert find_key(data, MyObj) == [["c", "d", "x"]]


def test_find_key_value():
    from jobflow.utils import find_key_value

    data = {"a": [0, {"b": 1, "x": 3}], "c": {"d": {"x": 3}}}
    result = find_key_value(data, "x", 3)
    assert len(result) == 2
    assert ["a", 1] in result
    assert ["c", "d"] in result


def test_update_in_dictionary():
    from jobflow.utils import update_in_dictionary

    data = {"a": [0, {"b": 1, "x": 3}], "c": {"d": {"x": 3}}}
    update_in_dictionary(data, {("a", 1, "x"): 100, ("c", "d", "x"): 100})
    assert data == {"a": [0, {"b": 1, "x": 100}], "c": {"d": {"x": 100}}}


def test_contains_job_or_flow():
    from jobflow import Flow, Job
    from jobflow.utils import contains_flow_or_job

    job = Job(str)
    flow = Flow([])

    assert contains_flow_or_job(True) is False
    assert contains_flow_or_job(1) is False
    assert contains_flow_or_job("abc") is False
    assert contains_flow_or_job(job) is True
    assert contains_flow_or_job(flow) is True
    assert contains_flow_or_job([flow]) is True
    assert contains_flow_or_job([[flow]]) is True
    assert contains_flow_or_job({"a": flow}) is True
    assert contains_flow_or_job({"a": [flow]}) is True
    assert contains_flow_or_job(job) is True
    assert contains_flow_or_job([job]) is True
    assert contains_flow_or_job([[job]]) is True
    assert contains_flow_or_job({"a": job}) is True
    assert contains_flow_or_job({"a": [job]}) is True
