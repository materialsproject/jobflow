"""Define the primary jobflow database interface."""

from __future__ import annotations

import typing

from maggma.core import Store
from monty.json import MSONable

from jobflow.core.reference import OnMissing

if typing.TYPE_CHECKING:
    from enum import Enum
    from pathlib import Path
    from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, Union

    from maggma.core import Sort

    obj_type = Union[str, Enum, Type[MSONable], List[Union[Enum, str, Type[MSONable]]]]
    save_type = Optional[Dict[str, obj_type]]
    load_type = Union[bool, Dict[str, Union[bool, obj_type]]]

__all__ = ["JobStore"]

T = typing.TypeVar("T", bound="JobStore")


class JobStore(Store):
    """Store intended to allow pushing and pulling documents into multiple stores.

    Parameters
    ----------
    docs_store
        Store for basic documents.
    additional_stores
        Additional stores to use for storing large data/specific objects. Given as a
        mapping of ``{store name: store}`` where store is a maggma :obj:`.Store` object.
    save
        Which items to save in additional stores when uploading documents. Given as a
        mapping of ``{store name: store_type}`` where ``store_type`` can be a dictionary
        key (string or enum), an :obj:`.MSONable` class, or a list of keys/classes.
    load
        Which items to load from additional stores when querying documents. Given as a
        mapping of ``{store name: store_type}`` where ``store_type`` can be `True``, in
        which case all saved items are loaded, a dictionary key (string or enum),
        an :obj:`.MSONable` class, or a list of keys/classes. Alternatively,
        ``load=True`` will automatically load all items from every additional store.
    """

    def __init__(
        self,
        docs_store: Store,
        additional_stores: Optional[Dict[str, Store]] = None,
        save: save_type = None,
        load: load_type = False,
    ):
        """ """
        self.docs_store = docs_store
        if additional_stores is None:
            self.additional_stores = {}
        else:
            self.additional_stores = additional_stores

        # enforce uuid key
        self.docs_store.key = "uuid"
        for additional_store in self.additional_stores.values():
            additional_store.key = "blob_id"

        if save is None or save is False:
            save = {}
        self.save = save

        if load is None:
            load = False
        self.load = load

        kwargs = {
            k: getattr(docs_store, k)
            for k in ("key", "last_updated_field", "last_updated_type")
        }
        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        """Get the name of the data source.

        Returns
        -------
        str
            A string representing this data source.
        """
        return f"JobStore-{self.docs_store.name}"

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Parameters
        ----------
        force_reset
            Whether to reset the connection or not.
        """
        self.docs_store.connect(force_reset=force_reset)
        for additional_store in self.additional_stores.values():
            additional_store.connect(force_reset=force_reset)

    def close(self):
        """Close any connections."""
        self.docs_store.close()
        for additional_store in self.additional_stores.values():
            additional_store.close()

    def count(self, criteria: Optional[Dict] = None) -> int:
        """
        Count the number of documents matching the query criteria.

        Parameters
        ----------
        criteria
            PyMongo filter for documents to count in.

        Returns
        -------
        int
            The number of documents matching the query.
        """
        return self.docs_store.count(criteria=criteria)

    def query(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        load: load_type = None,
    ) -> Iterator[Dict]:
        """
        Query the JobStore for documents.

        Parameters
        ----------
        criteria
            PyMongo filter for documents to search in.
        properties
            Properties to return in grouped documents.
        sort
            Dictionary of sort order for fields. Keys are field names and values are 1
            for ascending or -1 for descending.
        skip
            Number of documents to skip.
        limit
            Limit on the total number of documents returned.
        load
            Which items to load from additional stores. See ``JobStore`` constructor for
            more details.

        Yields
        ------
        Dict
            The documents.
        """
        from pydash import get

        from jobflow.utils.find import find_key, update_in_dictionary

        if load is None:
            load = self.load

        load_keys = _prepare_load(load)

        if isinstance(properties, (list, tuple)):
            properties += ["uuid", "index"]
        elif isinstance(properties, dict):
            properties.update({"uuid": 1, "index": 1})

        docs = self.docs_store.query(
            criteria=criteria, properties=properties, sort=sort, skip=skip, limit=limit
        )

        for doc in docs:
            if load_keys:
                # Process is
                # 1. Find the locations of all blob identifiers.
                # 2. Filter the locations based on the load criteria.
                # 3. Resolve all data blobs using the data store.
                # 4. Insert the data blobs into the document
                locations = find_key(doc, "blob_uuid")
                all_blobs = [get(doc, list(loc)) for loc in locations]
                grouped_blobs = _filter_blobs(all_blobs, locations, load_keys)
                to_insert = {}

                for store_name, (blobs, locs) in grouped_blobs.items():
                    store = self.additional_stores.get(store_name, None)

                    if store is None:
                        raise ValueError(
                            f"Unrecognised additional store name: {store_name}"
                        )

                    object_info = {b["blob_uuid"]: loc for b, loc in zip(blobs, locs)}
                    objects = store.query(
                        criteria={"blob_uuid": {"$in": list(object_info.keys())}},
                        properties=["blob_uuid", "data"],
                    )
                    object_map = {o["blob_uuid"]: o["data"] for o in objects}
                    inserts = {tuple(l): object_map[o] for o, l in object_info.items()}
                    to_insert.update(inserts)

                update_in_dictionary(doc, to_insert)

            yield doc

    def query_one(
        self,
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        load: load_type = None,
    ) -> Optional[Dict]:
        """
        Query the Store for a single document.

        Parameters
        ----------
        criteria
            PyMongo filter for documents to search.
        properties
            Properties to return in the document.
        sort
            Dictionary of sort order for fields. Keys are field names and values are 1
            for ascending or -1 for descending.
        load
            Which items to load from additional stores. See ``JobStore`` constructor for
            more details.

        Returns
        -------
        dict or None
            The document.
        """
        docs = self.query(
            criteria=criteria, properties=properties, load=load, sort=sort, limit=1
        )
        d = next(docs, None)
        return d

    def update(
        self,
        docs: Union[List[Dict], Dict],
        key: Union[List, str, None] = None,
        save: Union[bool, save_type] = None,
    ):
        """
        Update or insert documents into the Store.

        Parameters
        ----------
        docs
            The document or list of documents to update.
        key
            Field name(s) to determine uniqueness for a document, can be a list of
            multiple fields, a single field, or None if the Store's key field is to
            be used.
        save
            Which items to save in additional stores. See ``JobStore`` constructor for
            more details.
        """
        from collections import defaultdict
        from copy import deepcopy

        from monty.json import jsanitize
        from pydash import get

        from jobflow.utils.find import find_key, update_in_dictionary

        if save is None or save is True:
            save = self.save

        save_keys = _prepare_save(save)

        if not isinstance(docs, list):
            docs = [docs]

        if key is None:
            key = ["uuid", "index"]

        blob_data = defaultdict(list)
        dict_docs = []
        for doc in docs:
            doc = jsanitize(doc, strict=True, allow_bson=True)
            dict_docs.append(doc)

            if save_keys:
                locations = []
                for store_name, store_save in save_keys.items():
                    for save_key in store_save:
                        locations.extend(find_key(doc, save_key, include_end=True))

                    objects = [get(doc, list(loc)) for loc in locations]
                    object_map = dict(zip(map(tuple, locations), objects))
                    object_info = {
                        k: _get_blob_info(o, store_name) for k, o in object_map.items()
                    }
                    update_in_dictionary(doc, object_info)

                    # Now format blob data for saving in the data_store
                    for loc, data in object_map.items():
                        blob = deepcopy(object_info[loc])
                        blob.pop("store")
                        blob.update(
                            {
                                "data": data,
                                "job_uuid": doc["uuid"],
                                "job_index": doc["index"],
                            }
                        )
                        blob_data[store_name].append(blob)

        self.docs_store.update(dict_docs, key=key)

        for store_name, blobs in blob_data.items():
            store = self.additional_stores.get(store_name, None)

            if store is None:
                raise ValueError(f"Unrecognised additional store name: {store_name}")

            store.update(blobs, key="blob_uuid")

    def ensure_index(self, key: str, unique: bool = False) -> bool:
        """
        Try to create an index on document store and return True success.

        Parameters
        ----------
        key
            Single key to index.
        unique
            Whether or not this index contains only unique keys.

        Returns
        -------
        bool
            Whether the index exists/was created correctly.
        """
        return self.docs_store.ensure_index(key, unique=unique)

    def groupby(
        self,
        keys: Union[List[str], str],
        criteria: Optional[Dict] = None,
        properties: Union[Dict, List, None] = None,
        sort: Optional[Dict[str, Union[Sort, int]]] = None,
        skip: int = 0,
        limit: int = 0,
        load: load_type = None,
    ) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Group documents by keys.

        Parameters
        ----------
        keys
            Fields to group documents.
        criteria
            PyMongo filter for documents to search in.
        properties
            Properties to return in grouped documents
        sort
            Dictionary of sort order for fields. Keys are field names and values are 1
            for ascending or -1 for descending.
        skip
            Number of documents to skip.
        limit
            Limit on the total number of documents returned.
        load
            Which items to load from additional stores. See ``JobStore`` constructor for
            more details.

        Yields
        ------
        dict, list[dict]
            The documents as (key, documents) grouped by their keys.
        """
        from itertools import groupby

        from pydash import get, has, set_

        keys = keys if isinstance(keys, (list, tuple)) else [keys]

        if isinstance(properties, dict):
            # make sure all keys are in properties...
            properties.update(dict(zip(keys, [1] * len(keys))))
        elif properties is not None:
            properties.extend(keys)

        docs = self.query(
            properties=properties,
            criteria=criteria,
            sort=sort,
            skip=skip,
            limit=limit,
            load=load,
        )
        data = [doc for doc in docs if all(has(doc, k) for k in keys)]

        def grouping_keys(doc):
            return tuple(get(doc, k) for k in keys)

        for vals, group in groupby(sorted(data, key=grouping_keys), key=grouping_keys):
            doc: Dict[str, Any] = {}
            for k, v in zip(keys, vals):
                set_(doc, k, v)
            yield doc, list(group)

    def remove_docs(self, criteria: Dict):
        """
        Remove docs matching the criteria.

        Parameters
        ----------
        criteria
            Criteria for documents to remove.
        """
        docs = self.query(criteria, properties=["uuid", "index"])
        for doc in docs:
            for store in self.additional_stores.values():
                store.remove_docs({"job_uuid": doc["uuid"], "job_index": doc["index"]})
        self.docs_store.remove_docs(criteria)

    @property
    def collection(self):
        """Get the collection name."""
        return self.docs_store.collection

    def get_output(
        self,
        uuid: str,
        which: Union[str, int] = "last",
        load: load_type = False,
        cache: Optional[Dict[str, Any]] = None,
        on_missing: OnMissing = OnMissing.ERROR,
    ):
        """
        Get the output of a job UUID.

        Note that, unlike :obj:`JobStore.query`, this function will automatically
        try to resolve any output references in the job outputs.

        Parameters
        ----------
        uuid
            A job UUID.
        which
            If there are multiple job runs, which index to use. Options are:

            - ``"last"`` (default): Use the last job that ran.
            - ``"first"``: Use the first job that ran.
            - ``"all"``: Return all outputs, sorted with the lowest index first.
            - Alternatively, if an integer is specified, the output for this specific
              index will be queried.

        load
            Which items to load from additional stores. Setting to ``True`` will load
            all items stored in additional stores. See the ``JobStore`` constructor for
            more details.
        cache
            A dictionary cache to use for resolving of reference values.
        on_missing
            What to do if the output contains a reference and the reference cannot
            be resolved.

        Returns
        -------
        Any
            The output(s) for the job UUID.
        """
        from jobflow.core.reference import (
            find_and_get_references,
            find_and_resolve_references,
        )

        # TODO: Currently, OnMissing is not respected if a reference cycle is detected
        #       this could be fixed but will require more complicated logic just to
        #       catch a very unlikely event.

        if isinstance(which, int) or which in ("last", "first"):
            sort = -1 if which == "last" else 1

            criteria: Dict[str, Any] = {"uuid": uuid}
            if isinstance(which, int):
                # handle specific index specified
                criteria["index"] = which

            result = self.query_one(
                criteria=criteria,
                properties=["output"],
                sort={"index": sort},
                load=load,
            )

            if result is None:
                istr = f" (index: {which})" if isinstance(which, int) else ""
                raise ValueError(f"UUID: {uuid}{istr} has no outputs.")

            refs = find_and_get_references(result["output"])
            if any([ref.uuid == uuid for ref in refs]):
                raise RuntimeError("Reference cycle detected - aborting.")

            return find_and_resolve_references(
                result["output"], self, cache=cache, on_missing=on_missing
            )
        else:
            results = list(
                self.query(
                    criteria={"uuid": uuid},
                    properties=["output"],
                    sort={"index": 1},
                    load=load,
                )
            )

            if len(results) == 0:
                raise ValueError(f"UUID: {uuid} has no outputs.")

            results = [r["output"] for r in results]

            refs = find_and_get_references(results)
            if any([ref.uuid == uuid for ref in refs]):
                raise RuntimeError("Reference cycle detected - aborting.")

            return find_and_resolve_references(
                results, self, cache=cache, on_missing=on_missing
            )

    @classmethod
    def from_file(cls: Type[T], db_file: Union[str, Path], **kwargs) -> T:
        """
        Create an JobStore from a database file.

        Two options are supported for the database file. The file should be in json or
        yaml format.

        The simplest format is a monty dumped version of the store, generated using:

        >>> from monty.serialization import dumpfn
        >>> dumpfn("job_store.yaml", job_store)

        Alternatively, the file can contain the keys docs_store, additional_stores and
        any other keyword arguments supported by the :obj:`JobStore` constructor. The
        docs_store and additional stores are specified by the ``type`` key which must
        match a Maggma ``Store`` subclass, and the remaining keys are passed to
        the store constructor. For example, the following file would  create a
        :obj:`JobStore` with a ``MongoStore`` for docs and a ``GridFSStore`` as an
        additional store for data.

        .. code-block:: yaml

            docs_store:
              type: MongoStore
              database: jobflow_unittest
              collection_name: outputs
              host: localhost
              port: 27017
            additional_stores:
              data:
                type: GridFSStore
                database: jobflow_unittest
                collection_name: outputs_blobs
                host: localhost
                port: 27017

        Parameters
        ----------
        db_file
            Path to the file containing the credentials.
        **kwargs
            Additional keyword arguments that get passed to the JobStore constructor.
            These arguments are ignored if the file contains a monty serialised
            JobStore.

        Returns
        -------
        JobStore
            A JobStore.
        """
        from monty.serialization import loadfn

        store_info = loadfn(db_file)

        if isinstance(store_info, JobStore):
            return store_info

        return cls.from_dict_spec(store_info, **kwargs)

    @classmethod
    def from_dict_spec(cls, spec: dict, **kwargs) -> T:
        """
        Create an JobStore from a dict specification.

        .. note::
            This function is different to ``JobStore.from_dict`` which is used to load
            the monty serialised representation of the claas.

        The dictionary should contain the keys "docs_store", "additional_stores" and
        any other keyword arguments supported by the :obj:`JobStore` constructor. The
        docs_store and additional stores are specified by the ``type`` key which must
        match a Maggma ``Store`` subclass, and the remaining keys are passed to
        the store constructor. For example, the following file would  create a
        :obj:`JobStore` with a ``MongoStore`` for docs and a ``GridFSStore`` as an
        additional store for data.

        .. code-block:: yaml

            docs_store:
              type: MongoStore
              database: jobflow_unittest
              collection_name: outputs
              host: localhost
              port: 27017
            additional_stores:
              data:
                type: GridFSStore
                database: jobflow_unittest
                collection_name: outputs_blobs
                host: localhost
                port: 27017

        Parameters
        ----------
        spec
            The dictionary specification.
        **kwargs
            Additional keyword arguments that get passed to the JobStore constructor.

        Returns
        -------
        JobStore
            A JobStore.
        """
        import maggma.stores  # required to enable subclass searching

        if "docs_store" not in spec:
            raise ValueError("Unrecognised database file format.")

        def all_subclasses(cl):
            return set(cl.__subclasses__()).union(
                [s for c in cl.__subclasses__() for s in all_subclasses(c)]
            )

        all_stores = {s.__name__: s for s in all_subclasses(maggma.stores.Store)}

        docs_store_info = spec["docs_store"]
        docs_store_type = docs_store_info.pop("type")
        docs_store = all_stores[docs_store_type](**docs_store_info)

        additional_stores = {}
        if "additional_stores" in spec:
            for store_name, info in spec["additional_stores"].items():
                store_type = info.pop("type")
                additional_stores[store_name] = all_stores[store_type](**info)

        return cls(docs_store, additional_stores, **kwargs)


def _prepare_load(
    load: load_type,
) -> Union[bool, Dict[str, Union[bool, List[Union[str, Tuple[str, str]]]]]]:
    """Standardize load types."""
    from enum import Enum

    if isinstance(load, bool):
        return load

    new_load: Dict[str, Union[bool, List[Union[str, Tuple[str, str]]]]] = {}
    for store_name, store_load in load.items():
        if isinstance(store_load, bool):
            new_load[store_name] = store_load
        else:
            if not isinstance(store_load, (tuple, list)):
                store_load = [store_load]

            new_store_load = []
            for ltype in store_load:
                if isinstance(ltype, Enum):
                    new_store_load.append(ltype.value)
                elif not isinstance(ltype, str) and issubclass(ltype, MSONable):
                    new_store_load.append((ltype.__module__, ltype.__name__))
                else:
                    new_store_load.append(ltype)

            new_load[store_name] = new_store_load
    return new_load


def _prepare_save(
    save: Union[bool, save_type],
) -> Dict[str, List[Union[str, Type[MSONable]]]]:
    """Standardize save type."""
    from enum import Enum

    if isinstance(save, bool):
        return {}

    new_save = {}
    for store_name, store_save in save.items():
        if not isinstance(store_save, (tuple, list, bool)):
            store_save = [store_save]

        new_save[store_name] = [
            o.value if isinstance(o, Enum) else o for o in store_save
        ]
    return new_save


def _filter_blobs(
    blob_infos: List[Dict],
    locations: List[List[Any]],
    load: Union[bool, Dict[str, Union[bool, List[Union[str, Tuple[str, str]]]]]] = None,
) -> Dict[str, Tuple[List[Dict], List[List[Any]]]]:
    """Filter and group blobs."""
    from collections import defaultdict

    def _group_blobs(infos, locs):
        grouped = defaultdict(lambda: (list(), list()))
        for info, loc in zip(infos, locs):
            grouped[info["store"]][0].append(info)
            grouped[info["store"]][1].append(loc)
        return grouped

    if isinstance(load, bool):
        # return all blobs (load has to be True to get here)
        return _group_blobs(blob_infos, locations)

    new_blobs = []
    new_locations = []
    for store_name, store_load in load.items():
        for blob, location in zip(blob_infos, locations):
            if store_load is True:
                new_blobs.append(blob)
                new_locations.append(location)
            elif isinstance(store_load, bool):
                # could just write "elif store_load is False" but need this for mypy
                continue
            else:
                for ltype in store_load:
                    if (
                        isinstance(ltype, tuple)
                        and blob.get("@class", None) == ltype[1]
                        and blob.get("@module", None) == ltype[0]
                    ):
                        new_blobs.append(blob)
                        new_locations.append(location)
                    elif location[-1] == ltype:
                        new_blobs.append(blob)
                        new_locations.append(location)

    return _group_blobs(new_blobs, new_locations)


def _get_blob_info(obj: Any, store_name: str) -> Dict[str, str]:
    from jobflow.utils.uuid import suuid

    class_name = ""
    module_name = ""
    if isinstance(obj, dict) and "@class" in obj and "@module" in obj:
        class_name = obj["@class"]
        module_name = obj["@module"]

    return {
        "@class": class_name,
        "@module": module_name,
        "blob_uuid": suuid(),
        "store": store_name,
    }
