import json
from dataclasses import dataclass, field

import pytest
from monty.json import MontyDecoder, MSONable

from jobflow import Flow, Maker, job


@dataclass
class FilterMaker(Maker):
    name: str = "relax"
    value: int = 0

    @job
    def make(self, value=1):
        return value + self.value


@dataclass
class NestedFilterMaker(Maker):
    name: str = "parent"
    child: Maker = field(default_factory=lambda: FilterMaker(name="static"))
    value: int = 0

    @job
    def make(self, value=1):
        return value + self.value + self.child.value


@dataclass
class SelectionMaker(Maker):
    name: str = "group"
    value: int = 0
    relax: Maker = field(default_factory=lambda: FilterMaker(name="relax"))
    static: Maker = field(default_factory=lambda: FilterMaker(name="static"))
    other: Maker = field(default_factory=lambda: FilterMaker(name="other"))


@dataclass
class NonMakerChild(MSONable):
    name: str = "non-maker"
    value: int = 0


@dataclass
class NonMakerContainer(Maker):
    name: str = "container"
    child: MSONable = field(default_factory=NonMakerChild)
    value: int = 0


def test_existing_maker_name_class_nesting_and_dict_mod_controls():
    maker = NestedFilterMaker(child=FilterMaker(name="selected"), value=2)

    updated = maker.update_kwargs(
        {"_inc": {"value": 3}},
        name_filter="selected",
        class_filter=FilterMaker,
        dict_mod=True,
    )
    assert updated.value == 2
    assert updated.child.value == 3
    assert maker.child.value == 0

    root_only = maker.update_kwargs(
        {"value": 5}, class_filter=FilterMaker, nested=False
    )
    assert root_only.value == 2
    assert root_only.child.value == 0


def test_existing_job_flow_and_mson_controls_without_callback():
    original = FilterMaker(value=2)
    task = original.make(3)
    task.update_maker_kwargs({"value": 5}, name_filter="relax")
    assert task.maker.value == 5
    assert original.value == 2

    first_maker = FilterMaker(name="relax", value=1)
    second_maker = FilterMaker(name="static", value=2)
    flow = Flow([first_maker.make(3), Flow([second_maker.make(3)])])
    flow.update_maker_kwargs(
        {"_inc": {"value": 4}}, name_filter="static", dict_mod=True
    )
    assert flow[0].maker.value == 1
    assert flow[1][0].maker.value == 6
    assert second_maker.value == 2

    encoded = json.dumps(task.as_dict())
    restored = MontyDecoder().process_decoded(json.loads(encoded))
    assert restored.maker.value == 5


def test_maker_callback_filter_selects_positive_and_negative_names():
    maker = SelectionMaker()
    seen = []

    def is_named_relax_or_static(candidate):
        seen.append(candidate.name)
        return candidate.name in {"relax", "static"}

    updated = maker.update_kwargs(
        {"value": 7}, callback_filter=is_named_relax_or_static
    )

    assert updated.relax.value == 7
    assert updated.static.value == 7
    assert updated.other.value == 0
    assert maker.relax.value == 0
    assert maker.static.value == 0
    assert set(seen) == {"group", "relax", "static", "other"}

    negative = maker.update_kwargs(
        {"value": 9}, callback_filter=lambda candidate: candidate.name == "other"
    )
    assert negative.relax.value == 0
    assert negative.static.value == 0
    assert negative.other.value == 9


def test_callback_filter_explicit_none_and_true_acceptance():
    maker = FilterMaker()

    with_none = maker.update_kwargs({"value": 2}, callback_filter=None)
    accepted = maker.update_kwargs({"value": 3}, callback_filter=lambda candidate: True)

    assert with_none.value == 2
    assert accepted.value == 3


def test_callback_filter_ands_name_and_class_filters():
    maker = SelectionMaker(
        name="selected-parent",
        relax=FilterMaker(name="selected-child"),
        static=FilterMaker(name="other-child"),
    )
    seen = []

    def accept(candidate):
        seen.append(candidate.name)
        return True

    updated = maker.update_kwargs(
        {"value": 4},
        name_filter="selected",
        class_filter=FilterMaker,
        callback_filter=accept,
    )

    assert updated.value == 0
    assert updated.relax.value == 4
    assert updated.static.value == 0
    assert seen == ["selected-child"]


def test_rejected_parent_does_not_prune_nested_maker_and_nested_false_limits_scope():
    maker = NestedFilterMaker(child=FilterMaker(name="static"))
    updated = maker.update_kwargs(
        {"value": 3}, callback_filter=lambda candidate: candidate.name == "static"
    )
    assert updated.value == 0
    assert updated.child.value == 3

    root_only = maker.update_kwargs(
        {"value": 5},
        nested=False,
        callback_filter=lambda candidate: candidate.name == "static",
    )
    assert root_only.value == 0
    assert root_only.child.value == 0


def test_callback_filter_combines_with_dict_mod():
    maker = FilterMaker(value=2)
    updated = maker.update_kwargs(
        {"_inc": {"value": 4}}, dict_mod=True, callback_filter=lambda candidate: True
    )
    assert updated.value == 6
    assert maker.value == 2


def test_callback_filter_does_not_receive_non_maker_msonable_children():
    maker = NonMakerContainer()
    seen = []

    def record(candidate):
        seen.append(candidate)
        return False

    updated = maker.update_kwargs({"value": 1}, callback_filter=record)

    assert len(seen) == 1
    assert isinstance(seen[0], Maker)
    assert seen[0].name == "container"
    assert updated.child.value == 0


def test_callback_filter_exceptions_propagate_same_exception():
    maker = FilterMaker()
    error = LookupError("callback sentinel")

    def fail(candidate):
        raise error

    with pytest.raises(LookupError) as raised:
        maker.update_kwargs({"value": 2}, callback_filter=fail)

    assert raised.value is error


def test_job_callback_filter_handles_bound_maker_arguments_and_keywords():
    bound = NestedFilterMaker(child=FilterMaker(name="bound-child"))
    positional = FilterMaker(name="positional")
    keyword = FilterMaker(name="keyword")
    task = bound.make(1)
    task.update_maker_kwargs(
        {"value": 2},
        name_filter="bound-child",
        class_filter=FilterMaker,
        callback_filter=lambda candidate: False,
    )

    assert task.maker.value == 0
    assert task.maker.child.value == 0
    task.update_maker_kwargs(
        {"value": 2},
        name_filter="bound-child",
        class_filter=FilterMaker,
        callback_filter=lambda candidate: candidate.name == "bound-child",
    )
    assert task.maker.child.value == 2
    assert bound.value == 0
    assert bound.child.value == 0

    @job
    def consume(first, second):
        return first.value + second.value

    task = consume(positional, second=keyword)
    task.update_maker_kwargs(
        {"value": 2}, callback_filter=lambda candidate: candidate.name != "keyword"
    )
    assert task.function_args[0].value == 2
    assert task.function_kwargs["second"].value == 0
    assert positional.value == 0
    assert keyword.value == 0


def test_flow_callback_filter_selects_makers_in_nested_flows():
    first_maker = FilterMaker(name="relax")
    second_maker = FilterMaker(name="static")
    flow = Flow([first_maker.make(), Flow([second_maker.make()])])

    flow.update_maker_kwargs(
        {"value": 6},
        class_filter=FilterMaker,
        callback_filter=lambda candidate: candidate.name == "static",
    )

    assert flow[0].maker.value == 0
    assert flow[1][0].maker.value == 6
    assert first_maker.value == 0
    assert second_maker.value == 0


def test_callback_filter_is_not_retained_in_updated_serialized_job():
    task = FilterMaker().make()
    task.update_maker_kwargs(
        {"value": 8}, callback_filter=lambda candidate: candidate.name == "relax"
    )

    encoded = json.dumps(task.as_dict())
    restored = MontyDecoder().process_decoded(json.loads(encoded))

    assert restored.maker.value == 8
    serialized = json.loads(encoded)
    pending = [serialized]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            assert "callback_filter" not in value
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    assert "<lambda>" not in encoded


def test_flow_callback_filter_preserves_numerical_execution(memory_jobstore):
    flow = Flow([FilterMaker(value=4).make(3)])
    flow.update_maker_kwargs(
        {"value": 10}, callback_filter=lambda candidate: candidate.name == "relax"
    )

    from jobflow.managers.local import run_locally

    run_locally(flow, store=memory_jobstore, ensure_success=True)
    assert memory_jobstore.get_output(flow[0].uuid) == 13
