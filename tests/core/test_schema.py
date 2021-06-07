def test_definition():
    from typing import Dict, List, Tuple

    from pydantic import Field

    from jobflow import Schema

    class MySchema(Schema):
        a: int
        b: str
        c: None = 1

    assert MySchema.schema() is not None

    class MySchema(Schema):
        a: List[int]
        b: Dict[str, int]
        c: Tuple[int, str, float, List[int]]
        d: int = Field(...)
        e: str = Field(None)

    assert MySchema.schema() is not None


def test_creation():
    from typing import Dict, List, Tuple

    from pydantic import Field

    from jobflow import Schema

    class MySchema(Schema):
        a: int
        b: str
        c: None = 1

    schema = MySchema(a="1", b=1)
    assert schema.schema() is not None
    assert schema.a == 1
    assert schema.b == "1"

    class MySchema(Schema):
        a: List[int]
        b: Dict[str, int]
        c: Tuple[int, str, float, List[int]]
        d: int = Field(...)
        e: str = Field(None)

    schema = MySchema(a=(1, 2, 4), b={"a": "5"}, c=["1", 2, 3, ["1", "2"]], d=1)
    assert schema.schema() is not None
    assert schema.a == [1, 2, 4]
    assert schema.b == {"a": 5}
    assert schema.c == (1, "2", 3.0, [1, 2])


def test_creation_with_reference():
    from typing import Any, Dict, List, Tuple

    from pydantic import Field

    from jobflow import OutputReference, Schema

    ref1 = OutputReference("123")
    ref2 = OutputReference("1234", (("a", "a"),))

    class MySchema(Schema):
        a: int
        b: str
        c: None = 1

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
        d: int = Field(...)
        e: str = Field(None)

    # test list item replacement with reference
    schema = MySchema(a=(1, 2, ref1), b={"a": "5"}, c=["1", 2, 3, ["1", "2"]], d=1)
    assert schema.a == [1, 2, ref1]

    # test dict value replacement with reference
    schema = MySchema(a=(1, 2, 4), b={"a": ref2}, c=["1", 2, 3, ["1", "2"]], d=1)
    assert schema.b == {"a": ref2}

    # test nested list replacement with reference
    schema = MySchema(a=(1, 2, 4), b={"a": "5"}, c=["1", 2, 3, ref2], d=1)
    assert schema.c == (1, "2", 3.0, ref2)

    # test double nested list replacement with reference
    schema = MySchema(a=(1, 2, 4), b={"a": "5"}, c=["1", 2, 3, ["1", ref2]], d=1)
    assert schema.c == (1, "2", 3.0, [1, ref2])

    # Test None in dict
    class Test(Schema):
        e: Dict[str, Any] = Field(None)

    schema = Test(e={"a": None})
    assert schema.e == {"a": None}

    # Pydantic is broken with union and Any, keeping this here in case I run into this
    # bug again

    # from pydantic import BaseModel
    #
    # class Test(BaseModel):
    #     e: Dict[Union[int, str], Union[str, Any]] = Field(None)
    #
    #     class Config:
    #         arbitrary_types_allowed = True
    #         extra = "allow"
    #
    #     def as_dict(self):
    #         return self.dict()
    #
    # schema = Test(e={'a': None})
    # assert schema.e == {"a": None}
