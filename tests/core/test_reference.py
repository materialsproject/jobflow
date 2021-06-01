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
        _ = ref.args

    with pytest.raises(AttributeError):
        _ = ref.__fake_variable


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
    assert hash(OutputReference("123", (1, 2))) == hash(OutputReference("123", (1, 2)))


def test_eq():
    from jobflow import OutputReference

    assert OutputReference("123") == OutputReference("123")
    assert OutputReference("123") != OutputReference("1234")
    assert OutputReference("123", (1,)) == OutputReference("123", (1,))
    assert OutputReference("123", (1,)) != OutputReference("1234", (1,))
    assert OutputReference("123", (1,)) != OutputReference("123", (2,))
    assert OutputReference("123", (1,)) != OutputReference("123", (2, 3, 4))
    assert OutputReference("123", (1,)) != "OutputReference(123, 1)"


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
        ref.resolve()

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

    ref = OutputReference("123", ("__module__",))
    cache = {"123": OutputReference}
    assert ref.resolve(cache=cache) == "jobflow.core.reference"

    # test missing attribute throws error
    ref = OutputReference("123", ("b",))

    cache = {"123": [1234]}
    with pytest.raises(AttributeError):
        ref.resolve(cache=cache)


def test_resolve_references(memory_jobstore):
    from jobflow import OnMissing, OutputReference
    from jobflow.core.reference import resolve_references

    # resolve single using cache
    ref = OutputReference("123")
    cache = {"123": "xyz"}
    output = resolve_references([ref], cache=cache)
    assert len(output) == 1
    assert output[ref] == "xyz"

    # resolve multiple using cache
    ref1 = OutputReference("123")
    ref2 = OutputReference("1234")
    cache = {"123": "xyz", "1234": 101}
    output = resolve_references([ref1, ref2], cache=cache)
    assert len(output) == 2
    assert output[ref1] == "xyz"
    assert output[ref2] == 101

    # resolve group using cache
    ref1 = OutputReference("123", ("a",))
    ref2 = OutputReference("123", ("b",))
    ref3 = OutputReference("1234")
    cache = {"123": {"a": "xyz", "b": "abc"}, "1234": 101}
    output = resolve_references([ref1, ref2, ref3], cache=cache)
    assert len(output) == 3
    assert output[ref1] == "xyz"
    assert output[ref2] == "abc"
    assert output[ref3] == 101

    # test on missing
    ref1 = OutputReference("123")
    ref2 = OutputReference("1234")
    cache = {"123": "xyz"}
    output = resolve_references([ref1, ref2], cache=cache, on_missing=OnMissing.NONE)
    assert len(output) == 2
    assert output[ref1] == "xyz"
    assert output[ref2] is None

    cache = {"123": "xyz"}
    output = resolve_references([ref1, ref2], cache=cache, on_missing=OnMissing.PASS)
    assert len(output) == 2
    assert output[ref1] == "xyz"
    assert output[ref2] == ref2

    with pytest.raises(ValueError):
        resolve_references([ref1, ref2], cache={}, on_missing=OnMissing.ERROR)

    # resolve using store
    memory_jobstore.update({"uuid": "123", "index": 1, "output": "xyz"})
    output = resolve_references([ref], store=memory_jobstore)
    assert len(output) == 1
    assert output[ref] == "xyz"

    # resolve using store and empty cache
    cache = {}
    output = resolve_references([ref], store=memory_jobstore, cache=cache)
    assert len(output) == 1
    assert output[ref] == "xyz"

    # check cache supersedes store
    cache = {"123": 101}
    output = resolve_references([ref], store=memory_jobstore, cache=cache)
    assert len(output) == 1
    assert output[ref] == 101

    # test attributes
    ref = OutputReference("123", ("a", 1))
    cache = {"123": {"a": [5, 6, 7]}}
    output = resolve_references([ref], cache=cache)
    assert output[ref] == 6

    ref = OutputReference("123", ("__module__",))
    cache = {"123": OutputReference}
    output = resolve_references([ref], cache=cache)
    assert output[ref] == "jobflow.core.reference"


def test_find_and_get_references():
    from jobflow.core.reference import OutputReference, find_and_get_references

    ref1 = OutputReference("123")
    ref2 = OutputReference("1234", ("a",))

    # test single reference
    assert find_and_get_references(ref1) == (ref1,)

    # test list and tuple of references
    assert find_and_get_references([ref1]) == (ref1,)
    assert set(find_and_get_references([ref1, ref2])) == {ref1, ref2}
    assert set(find_and_get_references((ref1, ref2))) == {ref1, ref2}

    # test dictionary dictionary values
    assert find_and_get_references({"a": ref1}) == (ref1,)
    assert set(find_and_get_references({"a": ref1, "b": ref2})) == {ref1, ref2}

    # test nested
    assert set(find_and_get_references({"a": [ref1, ref2]})) == {ref1, ref2}
    assert set(find_and_get_references([{"a": ref1}, {"b": ref2}])) == {ref1, ref2}


def test_find_and_resolve_references(memory_jobstore):
    from jobflow.core.reference import (
        OnMissing,
        OutputReference,
        find_and_resolve_references,
    )

    ref1 = OutputReference("123")
    ref2 = OutputReference("1234", ("a",))
    cache = {"123": 101, "1234": {"a": "xyz", "b": 5}}

    # test no reference
    assert find_and_resolve_references(True, cache=cache) == True
    assert find_and_resolve_references("xyz", cache=cache) == "xyz"
    assert find_and_resolve_references([101], cache=cache) == [101]

    # test single reference
    assert find_and_resolve_references(ref1, cache=cache) == 101

    # test list and tuple of references
    assert find_and_resolve_references([ref1], cache=cache) == [101]
    assert find_and_resolve_references([ref1, ref2], cache=cache) == [101, "xyz"]

    # test dictionary dictionary values
    output = find_and_resolve_references({"a": ref1}, cache=cache)
    assert output == {"a": 101}
    output = find_and_resolve_references({"a": ref1, "b": ref2}, cache=cache)
    assert output == {
        "a": 101,
        "b": "xyz",
    }

    # test nested
    output = find_and_resolve_references({"a": [ref1, ref2]}, cache=cache)
    assert output == {"a": [101, "xyz"]}
    output = find_and_resolve_references([{"a": ref1}, {"b": ref2}], cache=cache)
    assert output == [
        {"a": 101},
        {"b": "xyz"},
    ]

    # test store, no cache
    memory_jobstore.update(
        [
            {"uuid": "123", "index": 1, "output": 101},
            {"uuid": "1234", "index": 1, "output": {"a": "xyz", "b": 5}},
        ]
    )
    output = find_and_resolve_references({"a": [ref1, ref2]}, store=memory_jobstore)
    assert output == {"a": [101, "xyz"]}

    # test store, blank cache
    cache = {}
    output = find_and_resolve_references(
        {"a": [ref1, ref2]}, store=memory_jobstore, cache=cache
    )
    assert output == {"a": [101, "xyz"]}
    assert cache["123"] == 101

    # test cache overrides store
    output = find_and_resolve_references(
        {"a": [ref1, ref2]}, store=memory_jobstore, cache={"123": 1}
    )
    assert output == {"a": [1, "xyz"]}

    # test on missing
    cache = {"123": 101}
    output = find_and_resolve_references(
        [ref1, ref2], cache=cache, on_missing=OnMissing.PASS
    )
    assert output == [101, ref2]
    output = find_and_resolve_references(
        [ref1, ref2], cache=cache, on_missing=OnMissing.NONE
    )
    assert output == [101, None]

    with pytest.raises(ValueError):
        find_and_resolve_references(
            [ref1, ref2], cache=cache, on_missing=OnMissing.ERROR
        )
