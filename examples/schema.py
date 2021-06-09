from pydantic import BaseModel, Field

from jobflow import job


class ComputeSchema(BaseModel):
    """Document defining job output schema."""

    total: float = Field(description="Sum of the inputs.")
    product: float = Field(description="Product of the inputs.")


@job(output_schema=ComputeSchema)
def compute(a: float, b: float):
    return ComputeSchema(total=a + b, product=a * b)


compute_job = compute(1.1, 2.2)
print(compute_job.output.total)
# OutputReference(8ff2a94e-7633-42e9-8aa0-8479801347d5, .total)

compute_job.output.not_in_schema
# AttributeError: ComputeSchema does not have property 'not_in_schema'.
