"""A Pydantic model for Jobstore document."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_serializer

from jobflow import Maker


class MakerData(BaseModel):
    """A Pydantic model for the Maker data."""

    maker: Maker = Field(
        description="The instance of the Maker used to generate the Job/Flow"
    )
    args: list = Field(description="The args passed to the make method of the Maker")
    kwargs: dict = Field(
        description="The kwargs passed to the make method of the Maker"
    )

    @field_serializer("maker", mode="plain")
    def ser_maker(self, value: Any) -> Any:
        """Serialize the Maker object to prevent pydantic serialization."""
        # serialize the object manually, otherwise pydantic always converts it to
        # the standard dataclass serialization.
        if isinstance(value, Maker):
            return value.as_dict()
        return value

    def make(self):
        """
        Generate the object from the Maker using the arguments.

        Returns
        -------
        Flow or Job
            The generated Flow or Job.
        """
        args = self.args or []
        kwargs = self.kwargs or {}
        return self.maker.make(*args, **kwargs)


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
    hosts: list[tuple[str, int]] = Field(
        None,
        description="The list of UUIDs of the hosts containing the job.",
    )
    name: str = Field(
        None,
        description="The name of the job.",
    )
    maker: MakerData | None = Field(
        None, description="The information of the Maker used to generate the Job/Flow"
    )
