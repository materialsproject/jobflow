import pytest


def test_bad_subclass():
    from dataclasses import dataclass

    from jobflow import Maker

    @dataclass
    class BadMaker(Maker):
        name: str = "BadMaker"

    with pytest.raises(NotImplementedError):
        BadMaker().make()

    @dataclass
    class BadMaker(Maker):

        a = 1

    with pytest.raises(NotImplementedError):
        BadMaker().name()


def test_required_arguments_works():
    from dataclasses import dataclass

    @dataclass
    class MyMaker:

        a: int
        name = "123"

    m = MyMaker(1)
    assert m.a == 1

    with pytest.raises(TypeError):
        MyMaker()


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
    assert add_job.function_args == (1, 2)
    assert add_job.maker == maker  # maker should match as all kwargs are the same
    assert str(add_job.function) == str(maker.make)

    # test updating maker class does not impact the job
    maker.name = "sum"
    assert add_job.maker != maker  # now makers should not match
    assert add_job.maker.name != "sum"


def test_flow_maker():
    from dataclasses import dataclass

    from jobflow import Flow, Maker, job

    @job
    def add(a, b):
        return a + b

    @dataclass
    class AddMaker(Maker):
        name: str = "add"

        def make(self, a, b):
            first = add(a, b)
            second = add(first.output, b)
            return Flow([first, second], second.output, name=self.name)

    maker = AddMaker()
    add_job = maker.make(1, 2)
    assert add_job.name == "add"
    assert len(add_job.jobs) == 2

    @dataclass
    class DoubleAddMaker(Maker):
        name: str = "add_add"
        add1: AddMaker = AddMaker()
        add2: AddMaker = AddMaker()

        def make(self, a, b):
            first = self.add1.make(a, b)
            second = self.add2.make(first.output, b)
            return Flow([first, second], second.output, name=self.name)

    maker = DoubleAddMaker()
    double_add_flow = maker.make(1, 2)
    assert double_add_flow.name == "add_add"
    assert len(double_add_flow.jobs) == 2
    assert isinstance(double_add_flow.jobs[0], Flow)
    assert isinstance(double_add_flow.jobs[1], Flow)


def test_update_kwargs():
    from dataclasses import dataclass

    from monty.json import MSONable

    from jobflow.core.job import Response, job
    from jobflow.core.maker import Maker

    # this is needed to get monty to deserialize them correctly
    global AddMaker
    global DetourMaker

    @dataclass
    class AddMaker(Maker):
        name: str = "add"
        c: int = 5

        @job
        def make(self, a, b):
            return a + b + self.c

    @dataclass
    class DetourMaker(Maker):
        name: str = "add"
        add_maker: Maker = AddMaker()

        def make(self, a, b):
            detour = self.add_maker.make(a, b)
            return Response(detour=detour)

    # test no filter
    maker = AddMaker()
    maker = maker.update_kwargs({"c": 10})
    assert maker.c == 10

    # test bad kwarg
    maker = AddMaker()
    with pytest.raises(TypeError):
        maker.update_kwargs({"d": 10})

    # test name filter
    maker = AddMaker()
    maker = maker.update_kwargs({"c": 10}, name_filter="add")
    assert maker.c == 10

    maker = AddMaker()
    maker = maker.update_kwargs({"c": 10}, name_filter="div")
    assert maker.c == 5

    # test class filter
    maker = AddMaker()
    maker = maker.update_kwargs({"c": 10}, class_filter=AddMaker)
    assert maker.c == 10

    maker = AddMaker()
    maker = maker.update_kwargs({"c": 10}, class_filter=maker)
    assert maker.c == 10

    maker = AddMaker()
    maker = maker.update_kwargs({"c": 10}, class_filter=list)
    assert maker.c == 5

    # test dict mod
    maker = AddMaker()
    maker = maker.update_kwargs({"_inc": {"c": 10}}, dict_mod=True)
    assert maker.c == 15

    # test nesting
    maker = DetourMaker()
    maker = maker.update_kwargs({"c": 10}, class_filter=AddMaker, nested=True)
    assert maker.add_maker.c == 10

    maker = DetourMaker()
    maker = maker.update_kwargs({"c": 10}, class_filter=AddMaker, nested=False)
    assert maker.add_maker.c == 5

    global NotAMaker
    global FakeDetourMaker

    @dataclass
    class NotAMaker(MSONable):
        name: str = "add"
        c: int = 5

        @job
        def make(self, a, b):
            return a + b + self.c

    @dataclass
    class FakeDetourMaker(Maker):
        name: str = "add"
        add_maker: MSONable = NotAMaker()

        def make(self, a, b):
            detour = self.add_maker.make(a, b)
            return Response(detour=detour)

    # test non maker dataclasses not updated
    maker = FakeDetourMaker()
    maker = maker.update_kwargs({"c": 10}, class_filter=NotAMaker, nested=True)
    assert maker.add_maker.c == 5
