"""A Pydantic model for Jobstore document."""

from typing import Any

from pydantic import BaseModel, Field


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
