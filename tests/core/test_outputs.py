from dataclasses import dataclass

import pytest


def test_outputs_subclassing():
    from activities import Outputs

    # test basic subclass
    @dataclass
    class MyOutputs(Outputs):
        parameter1: str
        parameter2: int

    outputs = MyOutputs("a", 1)
    assert MyOutputs.fields() == ("parameter1", "parameter2")
    assert list(outputs.items()) == [("parameter1", "a"), ("parameter2", 1)]

    # test subclass with optional params
    @dataclass
    class MyOutputs(Outputs):
        parameter1: str
        parameter2: int = None

    outputs = MyOutputs("a")
    assert MyOutputs.fields() == ("parameter1", "parameter2")
    assert list(outputs.items()) == [("parameter1", "a"), ("parameter2", None)]


def test_outputs_references():
    from activities import Outputs, Reference

    # test generating and accessing references
    @dataclass
    class MyOutputs(Outputs):
        parameter1: str
        parameter2: int = None

    outputs = MyOutputs.with_references()
    assert MyOutputs.fields() == ("parameter1", "parameter2")
    assert isinstance(outputs.parameter1, Reference)
    assert isinstance(outputs.parameter2, Reference)
    for _, value in outputs.items():
        assert isinstance(value, Reference)
    assert set(outputs.references) == {outputs.parameter1, outputs.parameter2}


def test_outputs_resolve(output_store):
    from activities import Outputs, Reference

    @dataclass
    class MyOutputs(Outputs):
        parameter1: str
        parameter2: int = None

    # first test resolving a class with no references
    outputs = MyOutputs("a", 1)
    resolved_outputs = outputs.resolve()
    assert list(resolved_outputs.items()) == [("parameter1", "a"), ("parameter2", 1)]

    # test resolving references using output cache
    outputs = MyOutputs.with_references()
    cache = {outputs.parameter1.uuid: {"parameter1": "a", "parameter2": 1}}
    resolved_outputs = outputs.resolve(output_cache=cache)
    assert list(resolved_outputs.items()) == [("parameter1", "a"), ("parameter2", 1)]

    # test handling of missing references
    outputs = MyOutputs.with_references()
    with pytest.raises(ValueError):
        outputs.resolve(output_cache={})

    # test no error
    resolved_outputs = outputs.resolve(output_cache={}, error_on_missing=False)
    for _, value in resolved_outputs.items():
        assert isinstance(value, Reference)

    # test resolving references using output store
    outputs = MyOutputs.with_references()
    data = {"uuid": outputs.parameter1.uuid, "parameter1": "a", "parameter2": 1}
    output_store.update(data, key="uuid")
    resolved_outputs = outputs.resolve(output_store=output_store)
    assert list(resolved_outputs.items()) == [("parameter1", "a"), ("parameter2", 1)]


def test_outputs_to_store(output_store):
    from uuid import uuid4

    from activities import Outputs

    @dataclass
    class MyOutputs(Outputs):
        parameter1: str
        parameter2: int = None

    uuid = uuid4()
    outputs = MyOutputs("a", 1)
    outputs.to_store(output_store, uuid)
    result = output_store.query_one({"uuid": str(uuid)})
    assert result["parameter1"] == "a"
    assert result["parameter2"] == 1
