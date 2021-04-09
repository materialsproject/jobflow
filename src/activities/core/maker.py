from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass

from monty.json import MontyDecoder, MSONable

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Optional, Type, Union

    import activities


@dataclass
class Maker(ABC, MSONable):
    name: str = "Maker"

    @abstractmethod
    def make(self, *args, **kwargs) -> Union[activities.Activity, activities.Job]:
        pass

    def update_kwargs(
        self,
        update: Dict[str, Any],
        name_filter: Optional[str] = None,
        class_filter: Optional[Type[Maker]] = None,
        nested: bool = True,
        dict_mod: bool = False,
    ):
        from pydash import get, set_

        from activities.utils.dict_mods import apply_mod
        from activities.utils.find import find_key

        d = self.as_dict()

        if isinstance(class_filter, Maker):
            # Maker instance supplied rather than a Maker class
            class_filter = class_filter.__class__

        if nested:
            # find and update makers in Maker kwargs. Process is:
            # 1. Look for any monty classes in serialized maker kwargs
            # 2. Regenerate the classes and check if they are a Maker
            # 3. Apply the updates
            # 4. Reconstruct initial maker kwargs

            # find all classes in the serialized maker kwargs
            locations = find_key(d, "@class")

            for location in locations:
                if len(location) == 0:
                    # skip the current maker class
                    continue

                nested_class = MontyDecoder().process_decoded(get(d, list(location)))

                if isinstance(nested_class, Maker):
                    # class is a Maker; apply the updates
                    nested_class = nested_class.update_kwargs(
                        update,
                        name_filter=name_filter,
                        class_filter=class_filter,
                        nested=nested,
                        dict_mod=dict_mod,
                    )

                    # update the serialized maker with the new kwarg
                    set_(d, list(location), nested_class.as_dict())

        if name_filter is not None and name_filter not in self.name:
            return self

        if class_filter is not None and not isinstance(self, class_filter):
            return self

        # if we get to here then we pass all the filters
        if dict_mod:
            apply_mod(update, d)
        else:
            d.update(update)

        return self.from_dict(d)
