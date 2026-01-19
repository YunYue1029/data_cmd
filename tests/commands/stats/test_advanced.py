"""
Tests for stats command - Advanced aggregation functions.

Functions tested: values, dc (distinct count), first, last
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestValues:
    """Tests for values() aggregation - collect unique values."""

    def test_values_basic(self, sample_user_info):
        """Collect unique values."""
        cmd = 'cache=user_info | stats values(role) as roles by department'
        result = CommandExecutor(cmd).execute()

        assert "department" in result.columns
        assert "roles" in result.columns

    def test_values_contains_expected(self, sample_user_info):
        """Values contains expected unique items."""
        cmd = 'cache=user_info | stats values(role) as roles by department'
        result = CommandExecutor(cmd).execute()

        for _, row in result.iterrows():
            dept = row["department"]
            expected_roles = sample_user_info[sample_user_info["department"] == dept]["role"].unique()
            assert set(row["roles"]) == set(expected_roles)


class TestDistinctCount:
    """Tests for dc() - distinct count."""

    def test_dc_basic(self, sample_web_logs):
        """Count distinct values."""
        cmd = 'cache=web_logs | stats dc(ip) as unique_ips'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["ip"].nunique()
        assert result["unique_ips"].iloc[0] == expected

    def test_dc_by_group(self, sample_web_logs):
        """Distinct count grouped by field."""
        cmd = 'cache=web_logs | stats dc(ip) as unique_ips by host'
        result = CommandExecutor(cmd).execute()

        for host in sample_web_logs["host"].unique():
            expected = sample_web_logs[sample_web_logs["host"] == host]["ip"].nunique()
            actual = result[result["host"] == host]["unique_ips"].iloc[0]
            assert actual == expected


class TestFirst:
    """Tests for first() aggregation."""

    def test_first_basic(self, sample_orders):
        """Get first value."""
        cmd = 'cache=orders | stats first(order_date) as first_order by customer_id'
        result = CommandExecutor(cmd).execute()

        assert "first_order" in result.columns
        assert len(result) > 0


class TestLast:
    """Tests for last() aggregation."""

    def test_last_basic(self, sample_orders):
        """Get last value."""
        cmd = 'cache=orders | stats last(order_date) as last_order by customer_id'
        result = CommandExecutor(cmd).execute()

        assert "last_order" in result.columns
        assert len(result) > 0


class TestFirstLast:
    """Tests for first() and last() together."""

    def test_first_last_together(self, sample_orders):
        """First and last in same query."""
        cmd = 'cache=orders | stats first(order_date) as first_order, last(order_date) as last_order by customer_id'
        result = CommandExecutor(cmd).execute()

        assert "first_order" in result.columns
        assert "last_order" in result.columns

