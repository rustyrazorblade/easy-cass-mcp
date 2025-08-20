"""Unit tests for CassandraVersion class.

Tests version comparison and string representation.
"""

import pytest

from ecm.cassandra_version import CassandraVersion


class TestCassandraVersion:
    """Tests for CassandraVersion class."""

    def test_creation(self):
        """Test CassandraVersion creation."""
        version = CassandraVersion(5, 0, 1)
        assert version.major == 5
        assert version.minor == 0
        assert version.patch == 1

    def test_string_representation(self):
        """Test string representation of CassandraVersion."""
        version = CassandraVersion(4, 0, 11)
        assert str(version) == "4.0.11"
        
        version = CassandraVersion(5, 0, 0)
        assert str(version) == "5.0.0"

    def test_repr(self):
        """Test detailed representation of CassandraVersion."""
        version = CassandraVersion(4, 1, 5)
        assert repr(version) == "CassandraVersion(major=4, minor=1, patch=5)"

    def test_equality_with_version(self):
        """Test equality comparison with another CassandraVersion."""
        v1 = CassandraVersion(5, 0, 0)
        v2 = CassandraVersion(5, 0, 0)
        v3 = CassandraVersion(4, 0, 0)
        
        assert v1 == v2
        assert not v1 == v3

    def test_equality_with_tuple(self):
        """Test equality comparison with tuple."""
        version = CassandraVersion(5, 0, 1)
        
        assert version == (5, 0, 1)
        assert not version == (5, 0, 0)
        assert not version == (4, 0, 1)
        
        # Wrong tuple length should not equal
        assert not version == (5, 0)
        assert not version == (5, 0, 1, 0)

    def test_less_than(self):
        """Test less than comparison."""
        v1 = CassandraVersion(4, 0, 0)
        v2 = CassandraVersion(5, 0, 0)
        v3 = CassandraVersion(4, 1, 0)
        v4 = CassandraVersion(4, 0, 1)
        
        assert v1 < v2
        assert v1 < v3
        assert v1 < v4
        assert not v2 < v1
        
        # Test with tuple
        assert v1 < (5, 0, 0)
        assert v1 < (4, 1, 0)
        assert v1 < (4, 0, 1)

    def test_less_than_invalid(self):
        """Test less than with invalid types."""
        version = CassandraVersion(5, 0, 0)
        
        with pytest.raises(ValueError):
            version < (5, 0)  # Wrong tuple length
            
        with pytest.raises(TypeError):
            version < "5.0.0"  # String comparison

    def test_greater_than(self):
        """Test greater than comparison."""
        v1 = CassandraVersion(5, 0, 0)
        v2 = CassandraVersion(4, 0, 0)
        v3 = CassandraVersion(5, 1, 0)
        v4 = CassandraVersion(5, 0, 1)
        
        assert v1 > v2
        assert v3 > v1
        assert v4 > v1
        assert not v2 > v1
        
        # Test with tuple
        assert v1 > (4, 0, 0)
        assert v1 > (4, 9, 9)
        assert not v1 > (5, 0, 0)

    def test_greater_than_invalid(self):
        """Test greater than with invalid types."""
        version = CassandraVersion(5, 0, 0)
        
        with pytest.raises(ValueError):
            version > (5, 0)  # Wrong tuple length
            
        with pytest.raises(TypeError):
            version > "5.0.0"  # String comparison

    def test_less_equal(self):
        """Test less than or equal comparison."""
        v1 = CassandraVersion(4, 0, 0)
        v2 = CassandraVersion(4, 0, 0)
        v3 = CassandraVersion(5, 0, 0)
        
        assert v1 <= v2
        assert v1 <= v3
        assert not v3 <= v1
        
        # Test with tuple
        assert v1 <= (4, 0, 0)
        assert v1 <= (5, 0, 0)

    def test_greater_equal(self):
        """Test greater than or equal comparison."""
        v1 = CassandraVersion(5, 0, 0)
        v2 = CassandraVersion(5, 0, 0)
        v3 = CassandraVersion(4, 0, 0)
        
        assert v1 >= v2
        assert v1 >= v3
        assert not v3 >= v1
        
        # Test with tuple
        assert v1 >= (5, 0, 0)
        assert v1 >= (4, 0, 0)

    def test_hash(self):
        """Test hash functionality for use in sets and dicts."""
        v1 = CassandraVersion(5, 0, 0)
        v2 = CassandraVersion(5, 0, 0)
        v3 = CassandraVersion(4, 0, 0)
        
        # Same versions should have same hash
        assert hash(v1) == hash(v2)
        
        # Different versions should (likely) have different hashes
        assert hash(v1) != hash(v3)
        
        # Should work in sets
        version_set = {v1, v2, v3}
        assert len(version_set) == 2  # v1 and v2 are equal
        
        # Should work as dict keys
        version_dict = {v1: "five", v3: "four"}
        assert version_dict[v2] == "five"  # v2 equals v1

    def test_as_tuple(self):
        """Test conversion back to tuple."""
        version = CassandraVersion(4, 1, 5)
        assert version.as_tuple() == (4, 1, 5)
        
        version = CassandraVersion(5, 0, 0)
        assert version.as_tuple() == (5, 0, 0)

    def test_comparison_with_ucs_min_version(self):
        """Test comparison with UCS_MIN_VERSION constant."""
        ucs_min = CassandraVersion(5, 0, 0)
        
        v1 = CassandraVersion(4, 0, 11)
        v2 = CassandraVersion(5, 0, 0)
        v3 = CassandraVersion(5, 0, 1)
        
        assert v1 < ucs_min
        assert v2 >= ucs_min
        assert v3 >= ucs_min
        assert v2 == ucs_min