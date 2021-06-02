"""Utilities for enumerations."""

from enum import Enum

__all__ = ["ValueEnum"]


class ValueEnum(Enum):
    """Enum that serializes to string as the value and can be compared against a str."""

    def __str__(self):
        """Get a string representation of the enum."""
        return str(self.value)

    def __eq__(self, other):
        """Compare to another enum for equality."""
        if type(self) == type(other) and self.value == other.value:
            return True
        else:
            return str(self.value) == str(other)

    def __hash__(self):
        """Get a hash of the enum."""
        return hash(str(self))

    def as_dict(self):
        """Create a serializable representation of the enum."""
        return str(self.value)
