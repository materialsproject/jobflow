from pydantic import BaseModel, create_model
import typing


BaseModelT = typing.TypeVar('BaseModelT', bound=BaseModel)


def allow_references(model: typing.Type[BaseModelT]) -> typing.Type[BaseModelT]:
    from activities.core.reference import Reference
    from copy import deepcopy

    field_definitions = {}
    for name, field in model.__fields__.items():
        optional_field_info = deepcopy(field.field_info)
        field_type = typing.Union[field.type_, Reference]
        field_definitions[name] = (field_type, optional_field_info)

    return create_model(model.__name__, **field_definitions, __config__=model.Config)


class Schema(BaseModel):

    def __new__(cls, **kwargs):
        return allow_references(cls)(**kwargs)

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
