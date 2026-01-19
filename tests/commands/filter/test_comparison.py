"""
Tests for filter/where command - Comparison operators.

Operators tested: =, ==, !=, >, <, >=, <=
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestEquality:
    """Tests for equality operators (=, ==)."""

    def test_equal_string(self):
        """Filter by string equality."""
        df = pd.DataFrame({
            "status": ["active", "inactive", "active", "pending"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where status = "active"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["status"] == "active")

    def test_equal_number(self):
        """Filter by numeric equality."""
        df = pd.DataFrame({
            "code": [200, 404, 200, 500, 200],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where code = 200'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3
        assert all(result["code"] == 200)

    def test_double_equal(self):
        """Filter using == operator."""
        df = pd.DataFrame({
            "value": [1, 2, 3, 2, 1],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where value == 2'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2


class TestNotEqual:
    """Tests for not equal operator (!=)."""

    def test_not_equal_string(self):
        """Filter by string not equal."""
        df = pd.DataFrame({
            "status": ["active", "inactive", "active"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where status != "inactive"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["status"] == "active")

    def test_not_equal_number(self):
        """Filter by numeric not equal."""
        df = pd.DataFrame({
            "code": [200, 404, 500],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where code != 200'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert 200 not in result["code"].values


class TestGreaterThan:
    """Tests for greater than operator (>)."""

    def test_greater_than_number(self):
        """Filter numbers greater than value."""
        df = pd.DataFrame({
            "value": [10, 50, 100, 150, 200],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where value > 100'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["value"] > 100)

    def test_greater_than_boundary(self):
        """Greater than does not include boundary."""
        df = pd.DataFrame({
            "value": [99, 100, 101],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where value > 100'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 1
        assert result["value"].iloc[0] == 101


class TestLessThan:
    """Tests for less than operator (<)."""

    def test_less_than_number(self):
        """Filter numbers less than value."""
        df = pd.DataFrame({
            "value": [10, 50, 100, 150],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where value < 100'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["value"] < 100)


class TestGreaterThanOrEqual:
    """Tests for greater than or equal operator (>=)."""

    def test_gte_number(self):
        """Filter numbers greater than or equal."""
        df = pd.DataFrame({
            "value": [99, 100, 101],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where value >= 100'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["value"] >= 100)


class TestLessThanOrEqual:
    """Tests for less than or equal operator (<=)."""

    def test_lte_number(self):
        """Filter numbers less than or equal."""
        df = pd.DataFrame({
            "value": [99, 100, 101],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where value <= 100'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["value"] <= 100)


class TestRangeFiltering:
    """Tests for range filtering using multiple comparisons."""

    def test_between_values(self, sample_web_logs):
        """Filter between two values."""
        cmd = 'cache=web_logs | where status_code >= 400 | where status_code < 500'
        result = CommandExecutor(cmd).execute()

        assert all((result["status_code"] >= 400) & (result["status_code"] < 500))

