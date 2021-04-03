import typing

from pydantic import BaseModel, create_model
from pydantic.typing import get_args


def with_reference(atype):
    from activities import Reference

    args = tuple([with_reference(a) for a in get_args(atype)])

    if hasattr(atype, "copy_with"):
        atype = atype.copy_with(args)

    return typing.Union[Reference, atype]


def allow_references(model):
    """
    Create a new BaseModel with the exact same fields as `model` but making them all optional
    """
    from copy import deepcopy
    field_definitions = {}

    for name, field in model.__fields__.items():
        optional_field_info = deepcopy(field.field_info)

        if hasattr(field.outer_type_, "copy_with"):
            field_type = field.outer_type_.copy_with(field.type_)
        else:
            field_type = field.type_

        if field_type is not None:
            field_type = with_reference(field_type)

        field_definitions[name] = (field_type, optional_field_info)

    return create_model(model.__name__, **field_definitions, __config__=model.Config)


class Schema(BaseModel):
    def __new__(cls, **kwargs):
        return allow_references(cls)(**kwargs)

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
