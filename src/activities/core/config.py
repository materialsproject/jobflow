from dataclasses import dataclass, field

from activities.core.util import ValueEnum


class JobOrder(ValueEnum):
    AUTO = "auto"
    LINEAR = "linear"


class ReferenceFallback(ValueEnum):
    ERROR = "error"
    NONE = "none"
    PASS = "pass"


@dataclass
class JobConfig:

    resolve_references: bool = True
    on_missing_references: ReferenceFallback = ReferenceFallback.ERROR
    manager_config: dict = field(default_factory=dict)
    expose_store: bool = False
