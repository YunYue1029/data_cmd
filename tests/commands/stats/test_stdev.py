"""
Tests for stats command - Standard deviation function.

Function tested: stdev
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestStdevBasic:
    """Tests for basic stdev functionality."""

    def test_stdev_single_field(self, sample_web_logs):
        """Standard deviation of a single field."""
        cmd = 'cache=web_logs | stats stdev(response_time) as std_response'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["response_time"].std()
        assert abs(result["std_response"].iloc[0] - expected) < 0.01

    def test_stdev_by_group(self, sample_web_logs):
        """Standard deviation grouped by field."""
        cmd = 'cache=web_logs | stats stdev(response_time) as std_response by host'
        result = CommandExecutor(cmd).execute()

        for host in sample_web_logs["host"].unique():
            expected = sample_web_logs[sample_web_logs["host"] == host]["response_time"].std()
            actual = result[result["host"] == host]["std_response"].iloc[0]
            assert abs(actual - expected) < 0.01


class TestStdevWithOtherAggregations:
    """Tests for stdev combined with other aggregations."""

    def test_stdev_with_avg(self, sample_web_logs):
        """Stdev combined with avg."""
        cmd = 'cache=web_logs | stats avg(response_time) as avg_rt, stdev(response_time) as std_rt by host'
        result = CommandExecutor(cmd).execute()

        assert "avg_rt" in result.columns
        assert "std_rt" in result.columns

    def test_stdev_for_threshold_calculation(self, sample_server_metrics):
        """Use stdev for threshold calculation (baseline + 2*stdev)."""
        cmd = 'cache=server_metrics | stats avg(cpu_usage) as baseline, stdev(cpu_usage) as std by host'
        result = CommandExecutor(cmd).execute()

        assert "baseline" in result.columns
        assert "std" in result.columns
        # All stdev values should be positive
        assert all(result["std"] >= 0)

