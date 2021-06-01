def test_value_enum():
    from jobflow.utils import ValueEnum

    class TempEnum(ValueEnum):
        A = "A"
        B = "B"

    assert str(TempEnum.A) == "A"
    assert str(TempEnum.B) == "B"

    assert TempEnum.A == "A"
    assert TempEnum.B == "B"

    assert TempEnum.A.as_dict() == "A"
    assert TempEnum.B.as_dict() == "B"
