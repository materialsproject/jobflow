"""A Pydantic model for Jobstore document."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FileData(BaseModel):
    """A Pydantic mode for files data in a JobStoreDocument."""

    name: str = Field(description="The name of the file that has been uploaded.")
    reference: str = Field(
        description="A unique reference used to refer the file in the FileStore."
    )
    store: str = Field(
        description="Name of the FileStore when the file has been stored."
    )
    metadata: dict[str, Any] | None = Field(
        None, description="A generic dictionary with metadata identifying the file."
    )
    path: str = Field(description="The relative path of the file that was stored.")


class JobStoreDocument(BaseModel):
    """A Pydantic model for Jobstore document."""

    uuid: str = Field(
        None, description="An unique identifier for the job. Generated automatically."
    )
    index: int = Field(
        None,
        description="The index of the job (number of times the job has been replaced).",
    )
    output: Any = Field(
        None,
        description="This is a reference to the future job output.",
    )
    completed_at: str = Field(None, description="The time the job was completed.")
    metadata: dict = Field(
        None,
        description="Metadata information supplied by the user.",
    )
    hosts: list[str] = Field(
        None,
        description="The list of UUIDs of the hosts containing the job.",
    )
    name: str = Field(
        None,
        description="The name of the job.",
    )
    files: list[FileData] | None = Field(
        None, description="List of files stored as output of the Job in a FileStore."
    )
