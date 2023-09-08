"""A Pydantic model for Jobstore document."""

import typing

from pydantic import BaseModel, Field


class JobStoreDocument(BaseModel):
    """A Pydantic model for Jobstore document."""

    uuid: str = Field(
        None, description="A unique identifier for the job. Generated automatically."
    )
    index: int = Field(
        None,
        description="The index of the job (number of times the job has been replaced.",
    )
    output: typing.Any = Field(
        None,
        description="This is a reference to the future job outpu.",
    )
    completed_at: str = Field(None, description="The time the job was completed.")
    metadata: dict = Field(
        None,
        description="Metadeta information supplied by the user.",
    )
    hosts: list[str] = Field(
        None,
        description="The list of UUIDs of the hosts containing the job.",
    )
    name: str = Field(
        None,
        description="The name of the job.",
    )
