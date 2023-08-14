"""Define base Maker class for creating jobs and flows."""

from __future__ import annotations

import typing
from dataclasses import dataclass

from monty.json import MontyDecoder, MSONable

if typing.TYPE_CHECKING:
    from typing import Any, Callable

    import jobflow

__all__ = ["Maker"]


@dataclass
class Maker(MSONable):
    """
    Base maker (factory) class for constructing :obj:`Job` and :obj:`Flow` objects.

    Note, this class is only an abstract implementation. To create a functioning Maker,
    one should  subclass :obj:`Maker`, define the ``make`` method and add a name field
    with a default value. See examples below.

    See Also
    --------
    .job, .Job, .Flow

    Examples
    --------
    Below we define a simple maker that generates a job to add two numbers. Next, we
    make a job using the maker. When creating a new maker it is essential to: 1)
    Add a name field with a default value (this controls the name of jobs produced
    by the maker) and 2) override the ``make`` method with a concrete implementation.

    >>> from dataclasses import dataclass
    >>> from jobflow import Maker, job
    >>> @dataclass
    ... class AddMaker(Maker):
    ...     name: str = "add"
    ...
    ...     @job
    ...     def make(self, a, b):
    ...         return a + b
    >>> maker = AddMaker()
    >>> add_job = maker.make(1, 2)
    >>> add_job.name
    "add"

    There are two key features of :obj:`Maker` objects that make them extremely
    powerful. The first is the ability to have class instance variables that
    are used inside the make function. For example, running the job below will produce
    the output ``6.5``.

    >>> @dataclass
    ... class AddMaker(Maker):
    ...     name: str = "add"
    ...     c: float = 1.5
    ...
    ...     @job
    ...     def make(self, a, b):
    ...         return a + b + self.c
    >>> maker = AddMaker(c=3.5)
    >>> add_job = maker.make(1, 2)

    .. Note::
        Jobs created by a Maker will inherit a copy of the Maker object. This means that
        if 1) a job is created, 2) the maker variables are changed, and 3) a new job is
        created, only the second job will reflect the updated maker variables.

    The second useful feature is the ability for delayed creation of jobs. For example,
    consider a job that creates another job during its execution. If the new job
    has lots of optional arguments, this will require passing lots of arguments to the
    first job. However, using a Maker, the kwargs of the job can be specified as maker
    kwargs and specified when the first job is created, making it much cleaner to
    customise the newly created job. For example, consider this job that first squares
    two numbers, then creates a new job to add them.

    >>> from jobflow import Response
    >>> @dataclass
    ... class SquareThenAddMaker(Maker):
    ...     name: str = "square then add"
    ...     add_maker: Maker = AddMaker()
    ...
    ...     @job
    ...     def make(self, a, b):
    ...         a_squared = a ** 2
    ...         b_squared = b ** 2
    ...         add_job = self.add_maker.make(a_squared, b_squared)
    ...         return Response(detour=add_job)
    >>> maker = SquareThenAddMaker()
    >>> square_add_job = maker.make(3, 4)

    The add job can be customised by specifying a custom ``AddMaker`` instance when
    the ``SquareThenAddMaker`` is initialized.

    >>> custom_add_maker = AddMaker(c=1000)
    >>> maker = SquareThenAddMaker(add_maker=custom_add_maker)

    This provides a very natural way to contain and control the kwargs for dynamically
    created jobs.

    Note that makers can also be used to create :obj:`Flow` objects. In this case,
    the only difference is that the ``make()`` method is not decorated using the ``job``
    decorator and the function should return a flow. For example:

    >>> from jobflow import Flow
    >>> @dataclass
    ... class DoubleAddMaker(Maker):
    ...     name: str = "double add"
    ...     add_maker: Maker = AddMaker()
    ...
    ...     def make(self, a, b):
    ...         add_first = self.add_maker.make(a, b)
    ...         add_second = self.add_maker.make(add_first.output, b)
    ...         return Flow([add_first, add_second], name=self.name)
    >>> maker = DoubleAddMaker()
    >>> double_add_job = maker.make(1, 2)
    """

    def make(self, *args, **kwargs) -> jobflow.Flow | jobflow.Job:
        """Make a job or a flow - must be overridden with a concrete implementation."""
        raise NotImplementedError

    @property
    def name(self):
        """Name of the Maker - must be overridden with a dataclass field."""
        raise NotImplementedError

    def update_kwargs(
        self,
        update: dict[str, Any],
        name_filter: str = None,
        class_filter: type[Maker] = None,
        nested: bool = True,
        dict_mod: bool = False,
    ):
        """
        Update the keyword arguments of the :obj:`.Maker`.

        .. Note::
            Note, the maker is not updated in place. Instead a new maker object is
            returned.

        Parameters
        ----------
        update
            The updates to apply.
        name_filter
            A filter for the Maker name. Only Makers with a matching name will be
            updated. Includes partial matches, e.g. "ad" will match a Maker with the
            name "adder".
        class_filter
            A filter for the maker class. Only Makers with a matching class will be
            updated. Note the class filter will match any subclasses.
        nested
            Whether to apply the updates to Maker objects that are themselves kwargs
            of a Maker object. See examples for more details.
        dict_mod
            Use the dict mod language to apply updates. See :obj:`.DictMods` for more
            details.

        Examples
        --------
        Consider the following Maker:

        >>> from dataclasses import dataclass
        >>> from jobflow import job, Maker
        >>> @dataclass
        ... class AddMaker(Maker):
        ...     name: str = "add"
        ...     number: float = 10
        ...
        ...     @job
        ...     def make(self, a):
        ...         return a + self.number
        >>> maker = AddMaker()

        The ``number`` argument could be updated using:

        >>> maker.update_kwargs({"number": 10})

        By default, the updates are applied to nested Makers. These are Makers
        which are present in the kwargs of another Maker. For example, consider this
        maker that first squares a number, then creates a new job to add it.

        >>> from jobflow import Response
        >>> @dataclass
        ... class SquareThenAddMaker(Maker):
        ...     name: str = "square then add"
        ...     add_maker: Maker = AddMaker()
        ...
        ...     @job
        ...     def make(self, a, b):
        ...         a_squared = a ** 2
        ...         add_job = self.add_maker.make(a_squared)
        ...         return Response(detour=add_job)
        >>> maker = SquareThenAddMaker()

        The following update will apply to the nested ``AddMaker`` in the kwargs of the
        ``SquareThenAddMaker``:

        >>> maker = maker.update_kwargs({"number": 10}, class_filter=AddMaker)

        However, if ``nested=False``, then the update will not be applied to the nested
        Maker:

        >>> maker = maker.update_kwargs(
        ...     {"number": 10}, class_filter=AddMaker, nested=False
        ... )
        """
        from jobflow.utils.dict_mods import apply_mod

        def _update_kwargs_func(maker: Maker):
            # Update the kwargs of the maker
            d = maker.as_dict()
            if dict_mod:
                apply_mod(update, d)
            else:
                d.update(update)
            return maker.from_dict(d)

        return recursive_call(
            self,
            func=_update_kwargs_func,
            name_filter=name_filter,
            class_filter=class_filter,
            nested=nested,
        )


def recursive_call(
    obj: Maker,
    func: Callable[[Maker], Maker],
    name_filter: str = None,
    class_filter: type[Maker] = None,
    nested: bool = True,
):
    """Recursively call a function on all Maker objects in the object.

    Parameters
    ----------
    obj
        The Maker object to call the function on.
    func
        The function to call a Maker object, if it matches the filters.
        This function must return a new Maker object.
    name_filter
        A filter for the Maker name. Only Makers with a matching name will be updated.
        Includes partial matches, e.g. "ad" will match a Maker with the name "adder".
    class_filter
        A filter for the maker class. Only Makers with a matching class will be
        updated. Note the class filter will match any subclasses.
    nested
        Whether to apply the updates to Maker objects that are themselves kwargs
        of a Maker object. See examples for more details.
    """
    from pydash import get, set_

    from jobflow.utils.find import find_key

    if isinstance(class_filter, Maker):
        # Maker instance supplied rather than a Maker class
        class_filter = class_filter.__class__

    def _filter(nested_obj: Maker):
        # Filter the Maker object
        if not isinstance(nested_obj, Maker):
            return False
        if name_filter is not None and name_filter not in nested_obj.name:
            return False
        if class_filter is not None and not isinstance(nested_obj, class_filter):
            return False
        return True

    d = obj.as_dict()

    # find and update makers in Maker kwargs. Process is:
    # 1. Look for any monty classes in serialized maker kwargs
    # 2. Regenerate the classes and check if they are a Maker
    # 3. Call the functions on the deepest classes first
    # 4. Call the function or update on the deepest classes and move up the tree
    # 5. Finally, replace the call/update the object itself

    # find all classes in the serialized maker kwargs
    # will only look at the top level if nested=False
    locations = find_key(d, "@class", nested=True) if nested else [[]]

    for location in sorted(
        locations, key=len, reverse=True
    ):  # should deserialize in order
        if len(location) == 0:
            continue
        nested_class = MontyDecoder().process_decoded(get(d, list(location)))
        if _filter(nested_class):
            # either update or call the function on the nested Maker
            modified_class = func(nested_class)
            if not isinstance(modified_class, Maker):
                raise ValueError(
                    "Function must return a Maker object. "
                    f"Got {type(modified_class)} instead."
                )
            # update the serialized maker with the new kwarg
            set_(d, list(location), modified_class.as_dict())

    # the top level must be processed separately since its constructor
    # might not be discoverable by MontyDecoder (kinda hacky)
    new_obj = obj.from_dict(d)
    if _filter(obj):
        new_obj = func(new_obj)
    return new_obj
