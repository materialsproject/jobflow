"""A Pydantic model for Jobstore document."""

from typing import Generic, TypeVar

from monty.json import MontyDecoder
from pydantic import BaseModel, Field, field_validator

T = TypeVar("T")


class JobStoreDocument(BaseModel, Generic[T]):
    """A Pydantic model for Jobstore document."""

    uuid: str = Field(
        None, description="An unique identifier for the job. Generated automatically."
    )
    index: int = Field(
        None,
        description="The index of the job (number of times the job has been replaced).",
    )
    output: T = Field(
        None,
        description="This is a reference to the future job output.",
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

    @field_validator("output", mode="before")
    @classmethod
    def reserialize_output(cls, v):
        """
        Pre-validator for the 'output' field.

        This method checks if the input 'v' is a dictionary with specific keys
        ('@module' and '@class'). If these keys are present, it reprocesses
        the input dictionary using MontyDecoder to deserialize it.

        Parameters
        ----------
        cls : Type[JobStoreDocument]
            The class this validator is applied to.
        v : Any
            The input value to validate.

        Returns
        -------
        Any
            The validated and potentially deserialized value.
        """
        if isinstance(v, dict) and "@module" in v and "@class" in v:
            v = MontyDecoder().process_decoded(v)
        return v
