def test_definition():
    from typing import Dict, List, Tuple

    from jobflow import Schema

    class MySchema(Schema):
        a: int
        b: str

    assert MySchema.schema() is not None

    class MySchema(Schema):
        a: List[int]
        b: Dict[str, int]
        c: Tuple[int, str, float, List[int]]

    assert MySchema.schema() is not None


def test_creation():
    from typing import Dict, List, Tuple

    from jobflow import Schema

    class MySchema(Schema):
        a: int
        b: str

    schema = MySchema(a="1", b=1)
    assert schema.schema() is not None
    assert schema.a == 1
    assert schema.b == "1"

    class MySchema(Schema):
        a: List[int]
        b: Dict[str, int]
        c: Tuple[int, str, float, List[int]]

    schema = MySchema(a=(1, 2, 4), b={"a": "5"}, c=["1", 2, 3, ["1", "2"]])
    assert schema.schema() is not None
    assert schema.a == [1, 2, 4]
    assert schema.b == {"a": 5}
    assert schema.c == (1, "2", 3.0, [1, 2])


def test_creation_with_reference():
    from typing import Dict, List, Tuple

    from jobflow import OutputReference, Schema

    ref1 = OutputReference("123")
    ref2 = OutputReference("1234", ("a",))

    class MySchema(Schema):
        a: int
        b: str

    # test basic replacement with reference
    schema = MySchema(a="1", b=ref1)
    assert schema.schema() is not None
    assert schema.a == 1
    assert schema.b == ref1

    # test basic double replacement with reference
    schema = MySchema(a=ref2, b=ref1)
    assert schema.schema() is not None
    assert schema.a == ref2
    assert schema.b == ref1

    class MySchema(Schema):
        a: List[int]
        b: Dict[str, int]
        c: Tuple[int, str, float, List[int]]

    # test list item replacement with reference
    schema = MySchema(a=(1, 2, ref1), b={"a": "5"}, c=["1", 2, 3, ["1", "2"]])
    assert schema.a == [1, 2, ref1]

    # test dict value replacement with reference
    schema = MySchema(a=(1, 2, 4), b={"a": ref2}, c=["1", 2, 3, ["1", "2"]])
    assert schema.b == {"a": ref2}

    # test nested list replacement with reference
    schema = MySchema(a=(1, 2, 4), b={"a": "5"}, c=["1", 2, 3, ref2])
    assert schema.c == (1, "2", 3.0, ref2)

    # test double nested list replacement with reference
    schema = MySchema(a=(1, 2, 4), b={"a": "5"}, c=["1", 2, 3, ["1", ref2]])
    assert schema.c == (1, "2", 3.0, [1, ref2])
