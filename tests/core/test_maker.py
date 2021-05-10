import pytest


def test_bad_subclass():
    from dataclasses import dataclass

    from jobflow import Maker

    @dataclass
    class BadMaker(Maker):
        name: str = "BadMaker"

    with pytest.raises(NotImplementedError):
        BadMaker().make()
