import pytest


def test_bad_subclass():
    from dataclasses import dataclass

    from jobflow import Maker

    @dataclass
    class BadMaker(Maker):
        name: str = "BadMaker"

    with pytest.raises(NotImplementedError):
        BadMaker().make()


def test_job_maker():
    from dataclasses import dataclass

    from jobflow.core.job import job
    from jobflow.core.maker import Maker

    @dataclass
    class AddMaker(Maker):
        name: str = "add"

        @job
        def make(self, a, b):
            return a + b

    maker = AddMaker()
    add_job = maker.make(1, 2)
    assert add_job.name == "add"
    assert add_job.function == maker.make
