"""A basic implementation of a FileStore."""

from __future__ import annotations

import shutil
from abc import ABCMeta, abstractmethod, abstractproperty
from pathlib import Path
from typing import TYPE_CHECKING, Any

from monty.json import MSONable

if TYPE_CHECKING:
    import io

    from maggma.stores.ssh_tunnel import SSHTunnel


class FileStore(MSONable, metaclass=ABCMeta):
    """Abstract class for a file store."""

    @abstractproperty
    def name(self) -> str:
        """Return a string representing this data source."""

    @abstractmethod
    def put(self, src: str | io.IOBase, dest: str) -> str:
        """
        Insert a file in the Store.

        Return the string reference that can be used to access
        the file again.
        """

    @abstractmethod
    def get(self, reference: str, dest: str | io.IOBase):
        """Fetch a file from the store using the reference."""

    @abstractmethod
    def remove(self, reference: str):
        """Remove a file from the store."""

    @abstractmethod
    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not
        """

    @abstractmethod
    def close(self):
        """Close any connections."""


class FileSystemFileStore(FileStore):
    """File store on the file system."""

    def __init__(self, path: str):
        self.path = Path(path)

    @property
    def name(self) -> str:
        """Return a string representing this data source."""
        return f"fs:{self.path}"

    def put(self, src: str | io.IOBase, dest: str) -> str:
        """
        Insert a file in the Store.

        Return the string reference that can be used to access
        the file again.
        """
        path_dest = self.path / dest

        path_dest.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(src, str):
            shutil.copy2(src, path_dest)
        else:
            with open(path_dest, "wb") as f:
                f.write(src.read())

        return dest

    def get(self, reference: str, dest: str | io.IOBase):
        """Fetch a file from the store using the reference."""
        if isinstance(dest, str):
            shutil.copy2(self.path / reference, dest)
        else:
            with open(self.path / reference, "rb") as f:
                dest.write(f.read())

    def remove(self, reference: str):
        """Remove a file from the store."""
        file_path = self.path / reference
        file_path.unlink(missing_ok=True)

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not
        """
        self.path.mkdir(exist_ok=True)

    def close(self):
        """Close any connections."""


class GridFSFileStore(FileStore):
    """GridFS store for files."""

    def __init__(
        self,
        database: str,
        collection_name: str,
        host: str = "localhost",
        port: int = 27017,
        username: str = "",
        password: str = "",
        compression: bool = False,
        ensure_metadata: bool = False,
        searchable_fields: list[str] | None = None,
        auth_source: str | None = None,
        mongoclient_kwargs: dict | None = None,
        ssh_tunnel: SSHTunnel | None = None,
        **kwargs,
    ):
        """
        Initialize a GridFS Store for binary data.

        Args:
            database: database name
            collection_name: The name of the collection.
                This is the string portion before the GridFS extensions
            host: hostname for the database
            port: port to connect to
            username: username to connect as
            password: password to authenticate as
            compression: compress the data as it goes into GridFS
            auth_source: The database to authenticate on. Defaults to the database name.
            ssh_tunnel: An SSHTunnel object to use.
        """
        self.database = database
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._coll: Any = None
        self.compression = compression
        self.ssh_tunnel = ssh_tunnel

        if auth_source is None:
            auth_source = self.database
        self.auth_source = auth_source
        self.mongoclient_kwargs = mongoclient_kwargs or {}

    @property
    def name(self) -> str:
        """Return a string representing this data source."""
        return f"gridfs://{self.host}/{self.database}/{self.collection_name}"

    def connect(self, force_reset: bool = False):
        """
        Connect to the source data.

        Args:
            force_reset: whether to reset the connection or not when the Store is
                already connected.
        """
        import gridfs
        from pymongo import MongoClient

        if not self._coll or force_reset:
            if self.ssh_tunnel is None:
                host = self.host
                port = self.port
            else:
                self.ssh_tunnel.start()
                host, port = self.ssh_tunnel.local_address

            conn: MongoClient = (
                MongoClient(
                    host=host,
                    port=port,
                    username=self.username,
                    password=self.password,
                    authSource=self.auth_source,
                    **self.mongoclient_kwargs,
                )
                if self.username != ""
                else MongoClient(host, port, **self.mongoclient_kwargs)
            )
            db = conn[self.database]
            self._coll = gridfs.GridFS(db, self.collection_name)

    @property
    def _collection(self):
        """Property referring to underlying pymongo collection."""
        if self._coll is None:
            raise RuntimeError(
                "Must connect Mongo-like store before attempting to use it"
            )
        return self._coll

    def close(self):
        """Close any connections."""
        self._coll = None
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.stop()

    def put(self, src: str | io.IOBase, dest: str) -> str:
        """
        Insert a file in the Store.

        Return the string reference that can be used to access
        the file again.
        """
        metadata = {"path": dest}
        if isinstance(src, str):
            with open(src, "rb") as f:
                oid = self._collection.put(f, metadata=metadata)
        else:
            oid = self._collection.put(src, metadata=metadata)

        return str(oid)

    def get(self, reference: str, dest: str | io.IOBase):
        """Fetch a file from the store using the reference."""
        from bson import ObjectId

        data = self._collection.find_one({"_id": ObjectId(reference)})
        if isinstance(dest, str):
            with open(dest, "wb") as f:
                f.write(data.read())
        else:
            dest.write(dest.read())

    def remove(self, reference: str):
        """Remove a file from the store."""
        from bson import ObjectId

        self._collection.delete({"_id": ObjectId(reference)})
