"""
Tests for stats command - Basic aggregation functions.

Functions tested: count, sum, avg, min, max
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestCount:
    """Tests for count aggregation."""

    def test_count_all_rows(self, sample_web_logs):
        """Count all rows in dataset."""
        cmd = 'cache=web_logs | stats count as total'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 1
        assert result["total"].iloc[0] == len(sample_web_logs)

    def test_count_by_single_field(self, sample_web_logs):
        """Count grouped by single field."""
        cmd = 'cache=web_logs | stats count as total by host'
        result = CommandExecutor(cmd).execute()

        expected_hosts = sample_web_logs["host"].nunique()
        assert len(result) == expected_hosts

    def test_count_by_multiple_fields(self, sample_web_logs):
        """Count grouped by multiple fields."""
        cmd = 'cache=web_logs | stats count as n by host, status_code'
        result = CommandExecutor(cmd).execute()

        expected_groups = sample_web_logs.groupby(["host", "status_code"]).ngroups
        assert len(result) == expected_groups


class TestSum:
    """Tests for sum aggregation."""

    def test_sum_single_field(self, sample_web_logs):
        """Sum a single numeric field."""
        cmd = 'cache=web_logs | stats sum(bytes) as total_bytes'
        result = CommandExecutor(cmd).execute()

        assert result["total_bytes"].iloc[0] == sample_web_logs["bytes"].sum()

    def test_sum_by_group(self, sample_web_logs):
        """Sum grouped by field."""
        cmd = 'cache=web_logs | stats sum(bytes) as total_bytes by host'
        result = CommandExecutor(cmd).execute()

        for host in sample_web_logs["host"].unique():
            expected = sample_web_logs[sample_web_logs["host"] == host]["bytes"].sum()
            actual = result[result["host"] == host]["total_bytes"].iloc[0]
            assert actual == expected


class TestAvg:
    """Tests for avg (average/mean) aggregation."""

    def test_avg_single_field(self, sample_web_logs):
        """Average of a single field."""
        cmd = 'cache=web_logs | stats avg(response_time) as avg_response'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["response_time"].mean()
        assert abs(result["avg_response"].iloc[0] - expected) < 0.01

    def test_avg_by_group(self, sample_web_logs):
        """Average grouped by field."""
        cmd = 'cache=web_logs | stats avg(response_time) as avg_response by host'
        result = CommandExecutor(cmd).execute()

        for host in sample_web_logs["host"].unique():
            expected = sample_web_logs[sample_web_logs["host"] == host]["response_time"].mean()
            actual = result[result["host"] == host]["avg_response"].iloc[0]
            assert abs(actual - expected) < 0.01


class TestMinMax:
    """Tests for min and max aggregations."""

    def test_min_single_field(self, sample_web_logs):
        """Minimum of a single field."""
        cmd = 'cache=web_logs | stats min(response_time) as min_response'
        result = CommandExecutor(cmd).execute()

        assert result["min_response"].iloc[0] == sample_web_logs["response_time"].min()

    def test_max_single_field(self, sample_web_logs):
        """Maximum of a single field."""
        cmd = 'cache=web_logs | stats max(response_time) as max_response'
        result = CommandExecutor(cmd).execute()

        assert result["max_response"].iloc[0] == sample_web_logs["response_time"].max()

    def test_min_max_together(self, sample_web_logs):
        """Min and max in same query."""
        cmd = 'cache=web_logs | stats min(response_time) as min_rt, max(response_time) as max_rt'
        result = CommandExecutor(cmd).execute()

        assert result["min_rt"].iloc[0] == sample_web_logs["response_time"].min()
        assert result["max_rt"].iloc[0] == sample_web_logs["response_time"].max()

    def test_min_max_by_group(self, sample_web_logs):
        """Min and max grouped by field."""
        cmd = 'cache=web_logs | stats min(response_time) as min_rt, max(response_time) as max_rt by host'
        result = CommandExecutor(cmd).execute()

        for host in sample_web_logs["host"].unique():
            host_data = sample_web_logs[sample_web_logs["host"] == host]
            row = result[result["host"] == host]
            assert row["min_rt"].iloc[0] == host_data["response_time"].min()
            assert row["max_rt"].iloc[0] == host_data["response_time"].max()


class TestMultipleAggregations:
    """Tests for multiple aggregations in single query."""

    def test_count_sum_avg(self, sample_web_logs):
        """Multiple aggregations without grouping."""
        cmd = 'cache=web_logs | stats count as n, sum(bytes) as total_bytes, avg(response_time) as avg_rt'
        result = CommandExecutor(cmd).execute()

        assert result["n"].iloc[0] == len(sample_web_logs)
        assert result["total_bytes"].iloc[0] == sample_web_logs["bytes"].sum()

    def test_multiple_aggregations_by_group(self, sample_web_logs):
        """Multiple aggregations with grouping."""
        cmd = 'cache=web_logs | stats count as n, sum(bytes) as total_bytes, avg(response_time) as avg_rt by host'
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "n" in result.columns
        assert "total_bytes" in result.columns
        assert "avg_rt" in result.columns

