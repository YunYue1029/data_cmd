"""
Tests for multi-index query - Combine multiple data sources using OR syntax.

Covers:
- Basic multi-index query syntax: (index="a" OR index="b")
- Multi-cache query syntax: (cache=a OR cache=b)
- Column alignment from multiple sources
- Multi-index with subsequent operations
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestBasicMultiIndex:
    """Tests for basic multi-index query syntax."""

    def test_two_indexes_simple(self):
        """
        Test basic multi-index query with two sources.
        
        (index="data1" OR index="data2") | stats count
        """
        df1 = pd.DataFrame({
            "id": [1, 2],
            "name": ["A", "B"],
        })
        df2 = pd.DataFrame({
            "id": [3, 4],
            "name": ["C", "D"],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = '(index="data1" OR index="data2") | stats count as total'
        result = CommandExecutor(cmd).execute()

        assert result["total"].iloc[0] == 4

    def test_three_indexes(self):
        """Test multi-index query with three sources."""
        df1 = pd.DataFrame({"value": [1, 2]})
        df2 = pd.DataFrame({"value": [3, 4]})
        df3 = pd.DataFrame({"value": [5, 6]})
        register_cache("idx1", df1)
        register_cache("idx2", df2)
        register_cache("idx3", df3)

        cmd = '(index="idx1" OR index="idx2" OR index="idx3") | stats sum(value) as total'
        result = CommandExecutor(cmd).execute()

        assert result["total"].iloc[0] == 21  # 1+2+3+4+5+6

    def test_cache_syntax(self):
        """
        Test multi-source with cache= syntax.
        
        (cache=data1 OR cache=data2) | stats count
        """
        df1 = pd.DataFrame({"x": [1, 2, 3]})
        df2 = pd.DataFrame({"x": [4, 5]})
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = '(cache=data1 OR cache=data2) | stats count as n'
        result = CommandExecutor(cmd).execute()

        assert result["n"].iloc[0] == 5


class TestMultiIndexColumnAlignment:
    """Tests for column alignment with multi-index queries."""

    def test_same_schema(self):
        """Multi-index with same schema preserves all rows."""
        df1 = pd.DataFrame({
            "host": ["server01", "server01"],
            "value": [10, 20],
        })
        df2 = pd.DataFrame({
            "host": ["server02", "server02"],
            "value": [30, 40],
        })
        register_cache("metrics1", df1)
        register_cache("metrics2", df2)

        cmd = '(index="metrics1" OR index="metrics2")'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 4
        assert set(result["host"]) == {"server01", "server02"}
        assert result["value"].sum() == 100

    def test_different_schema(self):
        """Multi-index with different schemas aligns columns."""
        df1 = pd.DataFrame({
            "id": [1, 2],
            "name": ["A", "B"],
        })
        df2 = pd.DataFrame({
            "id": [3, 4],
            "score": [85, 90],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = '(index="data1" OR index="data2")'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 4
        assert "id" in result.columns
        assert "name" in result.columns
        assert "score" in result.columns
        
        # name should be NaN for data2 rows
        assert pd.isna(result.iloc[2]["name"])
        # score should be NaN for data1 rows
        assert pd.isna(result.iloc[0]["score"])


class TestMultiIndexWithOperations:
    """Tests for multi-index combined with pipe operations."""

    def test_multi_index_with_filter(self):
        """Multi-index followed by filter."""
        df1 = pd.DataFrame({
            "status": ["ok", "error"],
            "count": [10, 5],
        })
        df2 = pd.DataFrame({
            "status": ["ok", "error"],
            "count": [20, 15],
        })
        register_cache("log1", df1)
        register_cache("log2", df2)

        cmd = '(index="log1" OR index="log2") | where status = "ok"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["status"] == "ok")

    def test_multi_index_with_stats(self):
        """Multi-index followed by stats aggregation."""
        df1 = pd.DataFrame({
            "host": ["web01", "web02"],
            "requests": [100, 200],
        })
        df2 = pd.DataFrame({
            "host": ["web01", "web03"],
            "requests": [150, 300],
        })
        register_cache("metrics1", df1)
        register_cache("metrics2", df2)

        cmd = '(index="metrics1" OR index="metrics2") | stats sum(requests) as total by host'
        result = CommandExecutor(cmd).execute()

        result_dict = dict(zip(result["host"], result["total"]))
        assert result_dict["web01"] == 250  # 100 + 150
        assert result_dict["web02"] == 200
        assert result_dict["web03"] == 300

    def test_multi_index_with_eval(self):
        """Multi-index followed by eval calculation."""
        df1 = pd.DataFrame({
            "source": ["A"],
            "value": [100],
        })
        df2 = pd.DataFrame({
            "source": ["B"],
            "value": [200],
        })
        register_cache("src1", df1)
        register_cache("src2", df2)

        cmd = '(index="src1" OR index="src2") | eval doubled=value*2'
        result = CommandExecutor(cmd).execute()

        assert "doubled" in result.columns
        assert set(result["doubled"]) == {200, 400}


class TestMultiIndexLogScenarios:
    """Tests for realistic log query scenarios with multi-index."""

    def test_combine_app_and_error_logs(self, sample_app_logs, sample_error_logs):
        """
        Combine application logs and error logs using multi-index syntax.
        
        (index="app_logs" OR index="error_logs") | stats count
        """
        cmd = '(index="app_logs" OR index="error_logs") | stats count as total'
        result = CommandExecutor(cmd).execute()

        expected_count = len(sample_app_logs) + len(sample_error_logs)
        assert result["total"].iloc[0] == expected_count

    def test_multi_index_with_coalesce(self, sample_app_logs, sample_error_logs):
        """
        Multi-index with field normalization using coalesce.
        
        First extract level from app_logs, then combine and normalize.
        """
        # Prepare app_logs with extracted level
        app_logs_ext = sample_app_logs.copy()
        app_logs_ext["level"] = ["INFO", "WARN", "ERROR", "INFO", "DEBUG",
                                 "ERROR", "INFO", "WARN", "INFO", "ERROR"]
        register_cache("app_logs_ext", app_logs_ext)

        cmd = '''(index="app_logs_ext" OR index="error_logs") | eval log_level=coalesce(level, severity)'''
        result = CommandExecutor(cmd).execute()

        assert "log_level" in result.columns
        # Should have values from both sources
        assert not result["log_level"].isna().all()

    def test_multi_index_full_pipeline(self, sample_app_logs, sample_error_logs):
        """
        Complete multi-index pipeline with coalesce and stats.
        
        (index="app_logs" OR index="error_logs") | 
        eval log_level=coalesce(level, severity) |
        stats count as n by host, log_level
        """
        # Prepare app_logs with host and level
        app_logs_ext = sample_app_logs.copy()
        app_logs_ext["host"] = "app01"
        app_logs_ext["level"] = ["INFO", "WARN", "ERROR", "INFO", "DEBUG",
                                 "ERROR", "INFO", "WARN", "INFO", "ERROR"]
        register_cache("app_logs_ext", app_logs_ext)

        cmd = '''(index="app_logs_ext" OR index="error_logs") | eval log_level=coalesce(level, severity) | stats count as n by host, log_level'''
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "log_level" in result.columns
        assert "n" in result.columns
        
        # Total count should match combined rows
        expected_total = len(app_logs_ext) + len(sample_error_logs)
        assert result["n"].sum() == expected_total


class TestMultiIndexEdgeCases:
    """Tests for edge cases in multi-index queries."""

    def test_single_index_in_parens(self):
        """Single index in parentheses should work like regular query."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        register_cache("data", df)

        cmd = '(index="data") | stats sum(value) as total'
        result = CommandExecutor(cmd).execute()

        assert result["total"].iloc[0] == 6

    def test_multi_index_empty_source(self):
        """Multi-index with one empty source."""
        df1 = pd.DataFrame({"value": [1, 2, 3]})
        df_empty = pd.DataFrame(columns=["value"])
        register_cache("data1", df1)
        register_cache("empty", df_empty)

        cmd = '(index="data1" OR index="empty") | stats count as n'
        result = CommandExecutor(cmd).execute()

        assert result["n"].iloc[0] == 3

