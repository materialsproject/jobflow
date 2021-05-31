import pytest


def test_access():
    from jobflow import OutputReference

    # test empty
    ref = OutputReference("123")
    assert ref.attributes == tuple()

    new_ref = ref.a
    assert new_ref.attributes == ("a",)
    assert new_ref.uuid == "123"
    assert isinstance(new_ref, OutputReference)

    new_ref = ref["a"]
    assert new_ref.attributes == ("a",)
    assert new_ref.uuid == "123"
    assert isinstance(new_ref, OutputReference)

    new_ref = ref[1]
    assert new_ref.attributes == (1,)
    assert new_ref.uuid == "123"
    assert isinstance(new_ref, OutputReference)

    # test filled
    ref = OutputReference("123", ("b",))

    new_ref = ref.a
    assert new_ref.attributes == ("b", "a")
    assert new_ref.uuid == "123"
    assert isinstance(new_ref, OutputReference)

    new_ref = ref["a"]
    assert new_ref.attributes == ("b", "a")
    assert new_ref.uuid == "123"
    assert isinstance(new_ref, OutputReference)

    new_ref = ref[1]
    assert new_ref.attributes == ("b", 1)
    assert new_ref.uuid == "123"
    assert isinstance(new_ref, OutputReference)

    with pytest.raises(AttributeError):
        ref.args

    with pytest.raises(AttributeError):
        ref.__fake_variable


def test_get_set_attr():
    from jobflow import OutputReference

    ref = OutputReference("123")

    # these should fail
    with pytest.raises(TypeError):
        ref["a"] = 1

    with pytest.raises(TypeError):
        ref[1] = 1

    with pytest.raises(TypeError):
        ref.a = 1

    ref.uuid = 1
    assert ref.uuid == 1


def test_repr():
    from jobflow import OutputReference

    ref = OutputReference("123")
    assert str(ref) == "OutputReference(123)"

    ref = OutputReference("123", ("a",))
    assert str(ref) == "OutputReference(123, 'a')"

    ref = OutputReference("123", ("a", 1))
    assert str(ref) == "OutputReference(123, 'a', 1)"


def test_hash():
    from jobflow import OutputReference

    assert hash(OutputReference("123")) == hash(OutputReference("123"))
    assert hash(OutputReference("123", [1, 2])) == hash(OutputReference("123", [1, 2]))


def test_eq():
    from jobflow import OutputReference

    assert OutputReference("123") == OutputReference("123")
    assert OutputReference("123") != OutputReference("1234")
    assert OutputReference("123", [1]) == OutputReference("123", [1])
    assert OutputReference("123", [1]) == OutputReference("123", (1,))
    assert OutputReference("123", [1]) != OutputReference("1234", [1])
    assert OutputReference("123", [1]) != OutputReference("123", [2])
    assert OutputReference("123", [1]) != OutputReference("123", [2, 3, 4])
    assert OutputReference("123", [1]) != "OutputReference(123, 1)"


def test_as_dict():
    from jobflow import OutputReference

    ref = OutputReference("123")
    d = ref.as_dict()
    assert d["@class"] == "OutputReference"
    assert d["@module"] == "jobflow.core.reference"
    assert d["uuid"] == "123"

    ref = OutputReference("123", ("a", "b"))
    d = ref.as_dict()
    assert d["@class"] == "OutputReference"
    assert d["@module"] == "jobflow.core.reference"
    assert d["uuid"] == "123"
    assert d["attributes"] == ("a", "b")


def test_set_uuid():
    from jobflow import OutputReference

    ref = OutputReference("123")
    new_ref = ref.set_uuid("321")
    assert ref.uuid == "321"
    assert new_ref.uuid == "321"

    ref = OutputReference("123")
    new_ref = ref.set_uuid("321", inplace=False)
    assert ref.uuid == "123"
    assert new_ref.uuid == "321"


def test_schema():
    from jobflow import OutputReference, Schema

    class MySchema(Schema):
        number: int
        name: str

    ref = OutputReference("123", output_schema=MySchema)
    assert ref.attributes == tuple()

    # check valid schema access works
    new_ref = ref.number
    assert new_ref.uuid == "123"
    assert new_ref.output_schema is None

    new_ref = ref["name"]
    assert new_ref.uuid == "123"
    assert new_ref.output_schema is None

    with pytest.raises(AttributeError):
        assert ref.a.uuid == "123"

    with pytest.raises(AttributeError):
        assert ref["a"].uuid == "123"

    with pytest.raises(AttributeError):
        assert ref[1].uuid == "123"


def test_resolve(memory_jobstore):
    from jobflow import OnMissing, OutputReference

    ref = OutputReference("123")

    # fail if cache or store not provided
    with pytest.raises(ValueError):
        assert ref.resolve()

    # resolve using cache
    cache = {"123": "xyz"}
    assert ref.resolve(cache=cache) == "xyz"

    # test on missing
    assert ref.resolve(cache={}, on_missing=OnMissing.NONE) is None
    assert ref.resolve(cache={}, on_missing=OnMissing.PASS) == ref

    with pytest.raises(ValueError):
        ref.resolve(cache={}, on_missing=OnMissing.ERROR)

    # resolve using store
    memory_jobstore.update({"uuid": "123", "index": 1, "output": 101})
    assert ref.resolve(store=memory_jobstore) == 101

    # resolve using store and empty cache
    cache = {}
    assert ref.resolve(store=memory_jobstore, cache=cache) == 101
    assert cache["123"] == 101

    # check cache supersedes store
    cache = {"123": "xyz"}
    assert ref.resolve(store=memory_jobstore, cache=cache) == "xyz"
    assert cache["123"] == "xyz"

    # test attributes
    ref = OutputReference("123", ("a", 1))
    cache = {"123": {"a": [5, 6, 7]}}
    assert ref.resolve(cache=cache) == 6

    # test missing attribute throws error
    ref = OutputReference("123", ("b",))
    cache = {"123": [1234]}
    with pytest.raises(TypeError):
        ref.resolve(cache=cache)
