"""
Tests for append command - Combine results from multiple sources.

Covers:
- Basic append with subquery
- Column alignment between sources
- Append with different schemas
- Append followed by aggregations
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestBasicAppend:
    """Tests for basic append operations."""

    def test_append_two_caches(self):
        """Append two cached DataFrames with same schema."""
        df1 = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "value": [100, 200, 300],
        })
        df2 = pd.DataFrame({
            "id": [4, 5, 6],
            "name": ["Dave", "Eve", "Frank"],
            "value": [400, 500, 600],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        # Should have all rows from both DataFrames
        assert len(result) == 6
        assert set(result["id"]) == {1, 2, 3, 4, 5, 6}

    def test_append_preserves_columns(self):
        """Append preserves all columns from both sources."""
        df1 = pd.DataFrame({
            "a": [1, 2],
            "b": ["x", "y"],
        })
        df2 = pd.DataFrame({
            "a": [3, 4],
            "b": ["z", "w"],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        assert "a" in result.columns
        assert "b" in result.columns
        assert result["a"].tolist() == [1, 2, 3, 4]

    def test_append_empty_source(self):
        """Append with empty source returns original data."""
        df1 = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        })
        df_empty = pd.DataFrame(columns=["id", "value"])
        register_cache("data1", df1)
        register_cache("empty_data", df_empty)

        cmd = 'cache=data1 | append [search index="empty_data"]'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3


class TestAppendColumnAlignment:
    """Tests for column alignment when appending."""

    def test_append_different_columns(self):
        """Append DataFrames with different columns fills NaN."""
        df1 = pd.DataFrame({
            "id": [1, 2],
            "name": ["A", "B"],
        })
        df2 = pd.DataFrame({
            "id": [3, 4],
            "value": [100, 200],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        # All columns should be present
        assert "id" in result.columns
        assert "name" in result.columns
        assert "value" in result.columns
        
        # First two rows should have name, last two should have NaN
        assert result.iloc[0]["name"] == "A"
        assert pd.isna(result.iloc[2]["name"])
        
        # First two rows should have NaN for value
        assert pd.isna(result.iloc[0]["value"])
        assert result.iloc[2]["value"] == 100

    def test_append_partial_overlap(self):
        """Append DataFrames with partial column overlap."""
        df1 = pd.DataFrame({
            "id": [1, 2],
            "name": ["A", "B"],
            "category": ["X", "Y"],
        })
        df2 = pd.DataFrame({
            "id": [3, 4],
            "name": ["C", "D"],
            "score": [85, 90],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 4
        assert set(result.columns) == {"id", "name", "category", "score"}


class TestAppendWithOperations:
    """Tests for append combined with other operations."""

    def test_append_then_stats(self):
        """Append followed by stats aggregation."""
        df1 = pd.DataFrame({
            "host": ["server01", "server01", "server02"],
            "value": [10, 20, 30],
        })
        df2 = pd.DataFrame({
            "host": ["server02", "server03", "server03"],
            "value": [40, 50, 60],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"] | stats sum(value) as total by host'
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "total" in result.columns
        
        # Check totals per host
        result_dict = dict(zip(result["host"], result["total"]))
        assert result_dict["server01"] == 30  # 10 + 20
        assert result_dict["server02"] == 70  # 30 + 40
        assert result_dict["server03"] == 110  # 50 + 60

    def test_append_then_filter(self):
        """Append followed by filter."""
        df1 = pd.DataFrame({
            "status": ["ok", "error"],
            "count": [10, 5],
        })
        df2 = pd.DataFrame({
            "status": ["ok", "error"],
            "count": [20, 15],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"] | where status = "error"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["status"] == "error")

    def test_append_then_eval(self):
        """Append followed by eval calculation."""
        df1 = pd.DataFrame({
            "source": ["A", "A"],
            "value": [100, 200],
        })
        df2 = pd.DataFrame({
            "source": ["B", "B"],
            "value": [300, 400],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"] | eval doubled=value*2'
        result = CommandExecutor(cmd).execute()

        assert "doubled" in result.columns
        assert result["doubled"].tolist() == [200, 400, 600, 800]


class TestAppendLogScenarios:
    """Tests for realistic log append scenarios."""

    def test_append_app_and_error_logs(self, sample_app_logs, sample_error_logs):
        """
        Append application logs and error logs.
        
        cache=app_logs | append [search index="error_logs"]
        """
        cmd = 'cache=app_logs | append [search index="error_logs"]'
        result = CommandExecutor(cmd).execute()

        # Should contain rows from both sources
        expected_rows = len(sample_app_logs) + len(sample_error_logs)
        assert len(result) == expected_rows

    def test_append_with_coalesce(self, sample_app_logs, sample_error_logs):
        """
        Append logs and normalize fields with coalesce.
        
        cache=app_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)" |
        append [search index="error_logs"] | 
        eval log_level=coalesce(level, severity)
        """
        cmd = '''cache=app_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)" | append [search index="error_logs"] | eval log_level=coalesce(level, severity)'''
        result = CommandExecutor(cmd).execute()

        assert "log_level" in result.columns

    def test_append_with_stats_by_host(self, sample_app_logs, sample_error_logs):
        """
        Append logs and get count by host.
        
        First extend app_logs with host column, then append and aggregate.
        """
        # Extend app_logs with host and level
        app_logs_ext = sample_app_logs.copy()
        app_logs_ext["host"] = "app01"
        app_logs_ext["level"] = ["INFO", "WARN", "ERROR", "INFO", "DEBUG",
                                 "ERROR", "INFO", "WARN", "INFO", "ERROR"]
        register_cache("app_logs_ext", app_logs_ext)

        cmd = '''cache=app_logs_ext | append [search index="error_logs"] | eval log_level=coalesce(level, severity) | stats count as n by host, log_level'''
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "log_level" in result.columns
        assert "n" in result.columns

