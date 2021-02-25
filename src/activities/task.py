from dataclasses import dataclass, field
from typing import Optional, Any, Dict, Tuple
from uuid import UUID, uuid4

from monty.json import MSONable, MontyEncoder

from activities.core import HasInputOutput
from activities.outputs import Outputs
from activities.reference import Reference


def task(method=None, outputs=None):
    """Wraps a function to produce a ``Task``."""

    def decorator(func):
        from functools import wraps

        @wraps(func)
        def get_task(*args, **kwargs) -> Task:
            module = func.__module__
            name = func.__name__
            return Task(function=(module, name), outputs=outputs, args=args,
                        kwargs=kwargs)

        return get_task

    if method:
        # This was an actual decorator call, ex: @cached_property
        return decorator(method)
    else:
        # This is a factory call, ex: @cached_property()
        return decorator


@dataclass
class Task(HasInputOutput, MSONable):

    function: Tuple[str, str]
    args: Tuple[Any] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    outputs: Optional[Outputs] = None
    uuid: UUID = field(default_factory=uuid4)

    def __post_init__(self):
        if self.outputs:
            self.outputs = self.outputs.reference(self.uuid)

    def __call__(self, *args, **kwargs):
        return Task(
            function=self.function,
            args=args,
            kwargs=kwargs,
            outputs=self.outputs.reference(self.uuid)
        )

    @property
    def input_references(self) -> Tuple[Reference, ...]:
        references = set()
        for arg in self.args + tuple(self.kwargs.values()):
            # TODO: could do this during init and store the references and their
            #   locations instead. Similarly, could store the serialized args and
            #   kwargs too.
            references.update(find_references(arg))

        return tuple(references)

    @property
    def output_references(self) -> Tuple[Reference, ...]:
        if self.outputs is None:
            return tuple()
        return self.outputs.references


def find_references(arg: Any) -> Tuple[Reference, ...]:
    from activities.util import find_key_value
    from pydash import get
    import json

    if isinstance(arg, Reference):
        # if the argument is a reference then stop there
        return tuple([arg])

    elif isinstance(arg, (float, int, str, bool)):
        # argument is a primitive, we won't find a reference here
        return tuple()

    # otherwise, serialize the argument to a dictionary
    arg = json.loads(MontyEncoder().encode(arg))

    # recursively find any reference classes
    locations = find_key_value(arg, "@class", "Reference")

    # deserialize references and return
    return tuple([Reference.from_dict(get(arg, loc)) for loc in locations])






