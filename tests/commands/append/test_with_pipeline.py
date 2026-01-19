"""
Tests for append command - Append with pipeline operations.

Append combined with other commands
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestAppendWithStats:
    """Tests for append combined with stats."""

    def test_append_then_stats(self):
        """Append followed by stats aggregation."""
        df1 = pd.DataFrame({"category": ["A", "A"], "value": [10, 20]})
        df2 = pd.DataFrame({"category": ["B", "B"], "value": [30, 40]})
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"] | stats sum(value) as total by category'
        result = CommandExecutor(cmd).execute()

        # Should have stats for both categories
        assert len(result) == 2
        assert "total" in result.columns

    def test_append_aggregated_data(self):
        """Append pre-aggregated data."""
        df1 = pd.DataFrame({
            "host": ["web01"],
            "metric": ["requests"],
            "value": [1000],
        })
        df2 = pd.DataFrame({
            "host": ["web01"],
            "metric": ["errors"],
            "value": [5],
        })
        register_cache("requests", df1)
        register_cache("errors", df2)

        cmd = 'cache=requests | append [search index="errors"]'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert set(result["metric"]) == {"requests", "errors"}


class TestAppendWithFilter:
    """Tests for append combined with filter."""

    def test_append_then_filter(self):
        """Append followed by filter."""
        df1 = pd.DataFrame({"name": ["Alice", "Bob"], "score": [85, 40]})
        df2 = pd.DataFrame({"name": ["Carol", "David"], "score": [95, 50]})
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"] | where score > 80'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["score"] > 80)


class TestAppendWithEval:
    """Tests for append combined with eval."""

    def test_append_then_eval(self):
        """Append followed by eval."""
        df1 = pd.DataFrame({"value": [10, 20]})
        df2 = pd.DataFrame({"value": [30, 40]})
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"] | eval doubled=value*2'
        result = CommandExecutor(cmd).execute()

        assert "doubled" in result.columns
        assert result["doubled"].tolist() == [20, 40, 60, 80]

