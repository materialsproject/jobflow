"""Define job output schema class."""

import typing

from monty.json import MSONable
from pydantic import BaseModel, create_model

if typing.TYPE_CHECKING:
    from typing import Type

__all__ = ["Schema"]


def with_reference(atype: Type):
    """
    Recursively redefine a type with the union of the type and :obj:`OutputReference`.

    For example, if the original type is ``Union[str, Tuple[str]]`` the modified
    type would be ``Union[str, OutputReference, Tuple[Union[str, OutputReference]]]``.

    Parameters
    ----------
    atype
        A type.

    Returns
    -------
    type
        A new type which is the recursive union with :obj:`OutputReference`.
    """
    from pydantic.typing import get_args

    from jobflow import OutputReference

    args = tuple([with_reference(a) for a in get_args(atype)])

    if hasattr(atype, "copy_with"):
        atype = atype.copy_with(args)

    return typing.Union[OutputReference, atype]


def allow_references(model: Type[BaseModel]):
    """
    Create new BaseModel with the same fields as ``model`` but that accepts References.

    Parameters
    ----------
    model
        A pydantic model.
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

    return create_model(
        model.__name__,
        **field_definitions,
        __base__=_BaseSchema,
        __module__=model.__module__
    )


class _BaseSchema(MSONable, BaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    def as_dict(self):
        return self.dict()


class Schema(_BaseSchema):
    """
    Base Schema class for representing job output schemas.

    This is a special pydantic model that automatically converts all types to support
    the :obj:`OutputReference` objects.
    """

    def __new__(cls, **kwargs):
        """Allow new instances to support :obj:`OutputReference` objects."""
        t = allow_references(cls)(**kwargs)
        return t
