"""
Tests for individual pipe commands.
"""

import pandas as pd
import pytest

from executors import CommandExecutor, register_cache


class TestStatsCommand:
    """Test cases for stats command."""

    def test_stats_count_by_field(self, register_simple: pd.DataFrame):
        """Test stats count by field."""
        cmd = "cache=data | stats count by department"
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3  # Sales, IT, HR
        assert "count" in result.columns
        assert result["count"].sum() == 5

    def test_stats_sum_by_field(self, register_simple: pd.DataFrame):
        """Test stats sum by field."""
        cmd = "cache=data | stats sum(salary) as total by department"
        result = CommandExecutor(cmd).execute()

        assert "total" in result.columns
        # IT: 60000 + 55000 = 115000
        it_total = result[result["department"] == "IT"]["total"].iloc[0]
        assert it_total == 115000

    def test_stats_multiple_aggregations(self, register_simple: pd.DataFrame):
        """Test stats with multiple aggregations."""
        cmd = "cache=data | stats count, sum(salary) as total, avg(age) as avg_age by department"
        result = CommandExecutor(cmd).execute()

        assert "count" in result.columns
        assert "total" in result.columns
        assert "avg_age" in result.columns


class TestFilterCommand:
    """Test cases for filter command."""

    def test_filter_equals(self, register_simple: pd.DataFrame):
        """Test filter with equals condition."""
        cmd = 'cache=data | filter department == "IT"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["department"] == "IT")

    def test_filter_greater_than(self, register_simple: pd.DataFrame):
        """Test filter with greater than condition."""
        cmd = "cache=data | filter salary > 50000"
        result = CommandExecutor(cmd).execute()

        assert all(result["salary"] > 50000)


class TestHeadTailCommand:
    """Test cases for head and tail commands."""

    def test_head(self, register_simple: pd.DataFrame):
        """Test head command."""
        cmd = "cache=data | head 3"
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3

    def test_tail(self, register_simple: pd.DataFrame):
        """Test tail command."""
        cmd = "cache=data | tail 2"
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2


class TestSortCommand:
    """Test cases for sort command."""

    def test_sort_ascending(self, register_simple: pd.DataFrame):
        """Test sort ascending."""
        cmd = "cache=data | sort salary"
        result = CommandExecutor(cmd).execute()

        salaries = result["salary"].tolist()
        assert salaries == sorted(salaries)

    def test_sort_descending(self, register_simple: pd.DataFrame):
        """Test sort descending."""
        cmd = "cache=data | sort -salary"
        result = CommandExecutor(cmd).execute()

        salaries = result["salary"].tolist()
        assert salaries == sorted(salaries, reverse=True)


class TestSelectCommand:
    """Test cases for select command."""

    def test_select_columns(self, register_simple: pd.DataFrame):
        """Test select specific columns."""
        cmd = "cache=data | select name, salary"
        result = CommandExecutor(cmd).execute()

        assert list(result.columns) == ["name", "salary"]


class TestRenameCommand:
    """Test cases for rename command."""

    def test_rename_column(self, register_simple: pd.DataFrame):
        """Test rename column."""
        cmd = "cache=data | rename salary as income"
        result = CommandExecutor(cmd).execute()

        assert "income" in result.columns
        assert "salary" not in result.columns


class TestEvalCommand:
    """Test cases for eval command."""

    def test_eval_arithmetic(self, register_simple: pd.DataFrame):
        """Test eval with arithmetic expression."""
        cmd = "cache=data | eval monthly = salary / 12"
        result = CommandExecutor(cmd).execute()

        assert "monthly" in result.columns
        expected = register_simple["salary"] / 12
        pd.testing.assert_series_equal(
            result["monthly"], expected, check_names=False
        )


class TestDedupCommand:
    """Test cases for dedup command."""

    def test_dedup_single_field(self, register_simple: pd.DataFrame):
        """Test dedup on single field."""
        cmd = "cache=data | dedup department"
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3  # Sales, IT, HR unique


class TestTopRareCommand:
    """Test cases for top and rare commands."""

    def test_top(self, register_simple: pd.DataFrame):
        """Test top command."""
        cmd = "cache=data | top department"
        result = CommandExecutor(cmd).execute()

        # Should have count column
        assert "count" in result.columns

    def test_rare(self, register_simple: pd.DataFrame):
        """Test rare command."""
        cmd = "cache=data | rare department"
        result = CommandExecutor(cmd).execute()

        assert "count" in result.columns

