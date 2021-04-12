from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from typing import Tuple, Type


def serialize_class(cls: Type) -> Tuple[str, str]:
    import inspect

    if not inspect.isclass(cls):
        raise ValueError("cls must be a class")

    return cls.__module__, cls.__name__


def deserialize_class(cls_spec: Tuple[str, str]):
    from importlib import import_module

    module = import_module(cls_spec[0])
    return getattr(module, cls_spec[1])
