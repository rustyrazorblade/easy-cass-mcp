"""Cassandra version representation and comparison utilities.

This module provides a CassandraVersion class for representing
and comparing Cassandra version numbers.
"""

from typing import Any


class CassandraVersion:
    """Represents a Cassandra version with major, minor, and patch components."""

    def __init__(self, major: int, minor: int, patch: int) -> None:
        """Initialize a CassandraVersion.
        
        Args:
            major: Major version number
            minor: Minor version number  
            patch: Patch version number
        """
        self.major = major
        self.minor = minor
        self.patch = patch

    def __str__(self) -> str:
        """Return string representation of the version."""
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self) -> str:
        """Return detailed representation of the version."""
        return f"CassandraVersion(major={self.major}, minor={self.minor}, patch={self.patch})"

    def __eq__(self, other: Any) -> bool:
        """Check if this version equals another."""
        if not isinstance(other, (CassandraVersion, tuple)):
            return False
        
        if isinstance(other, tuple):
            if len(other) != 3:
                return False
            return self.major == other[0] and self.minor == other[1] and self.patch == other[2]
        
        return self.major == other.major and self.minor == other.minor and self.patch == other.patch

    def __lt__(self, other: Any) -> bool:
        """Check if this version is less than another."""
        if isinstance(other, tuple):
            if len(other) != 3:
                raise ValueError(f"Cannot compare CassandraVersion with tuple of length {len(other)}")
            other_tuple = other
        elif isinstance(other, CassandraVersion):
            other_tuple = (other.major, other.minor, other.patch)
        else:
            raise TypeError(f"Cannot compare CassandraVersion with {type(other)}")
        
        return (self.major, self.minor, self.patch) < other_tuple

    def __le__(self, other: Any) -> bool:
        """Check if this version is less than or equal to another."""
        return self == other or self < other

    def __gt__(self, other: Any) -> bool:
        """Check if this version is greater than another."""
        if isinstance(other, tuple):
            if len(other) != 3:
                raise ValueError(f"Cannot compare CassandraVersion with tuple of length {len(other)}")
            other_tuple = other
        elif isinstance(other, CassandraVersion):
            other_tuple = (other.major, other.minor, other.patch)
        else:
            raise TypeError(f"Cannot compare CassandraVersion with {type(other)}")
        
        return (self.major, self.minor, self.patch) > other_tuple

    def __ge__(self, other: Any) -> bool:
        """Check if this version is greater than or equal to another."""
        return self == other or self > other

    def __hash__(self) -> int:
        """Return hash of the version for use in sets and dicts."""
        return hash((self.major, self.minor, self.patch))

    def as_tuple(self) -> tuple[int, int, int]:
        """Return the version as a tuple for backwards compatibility.
        
        Returns:
            Tuple of (major, minor, patch)
        """
        return (self.major, self.minor, self.patch)