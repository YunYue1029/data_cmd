"""
Tests for stats command - Percentile functions.

Functions tested: perc50, perc75, perc90, perc95, perc99
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestPerc50:
    """Tests for 50th percentile (median)."""

    def test_perc50_basic(self, sample_web_logs):
        """50th percentile of a field."""
        cmd = 'cache=web_logs | stats perc50(response_time) as p50'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["response_time"].quantile(0.50)
        assert abs(result["p50"].iloc[0] - expected) < 0.01

    def test_perc50_by_group(self, sample_web_logs):
        """50th percentile grouped by field."""
        cmd = 'cache=web_logs | stats perc50(response_time) as p50 by host'
        result = CommandExecutor(cmd).execute()

        assert len(result) == sample_web_logs["host"].nunique()


class TestPerc75:
    """Tests for 75th percentile."""

    def test_perc75_basic(self, sample_web_logs):
        """75th percentile of a field."""
        cmd = 'cache=web_logs | stats perc75(response_time) as p75'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["response_time"].quantile(0.75)
        assert abs(result["p75"].iloc[0] - expected) < 0.01


class TestPerc90:
    """Tests for 90th percentile."""

    def test_perc90_basic(self, sample_web_logs):
        """90th percentile of a field."""
        cmd = 'cache=web_logs | stats perc90(response_time) as p90'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["response_time"].quantile(0.90)
        assert abs(result["p90"].iloc[0] - expected) < 0.01


class TestPerc95:
    """Tests for 95th percentile."""

    def test_perc95_basic(self, sample_web_logs):
        """95th percentile of a field."""
        cmd = 'cache=web_logs | stats perc95(response_time) as p95'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["response_time"].quantile(0.95)
        assert abs(result["p95"].iloc[0] - expected) < 0.01


class TestPerc99:
    """Tests for 99th percentile."""

    def test_perc99_basic(self, sample_web_logs):
        """99th percentile of a field."""
        cmd = 'cache=web_logs | stats perc99(response_time) as p99'
        result = CommandExecutor(cmd).execute()

        expected = sample_web_logs["response_time"].quantile(0.99)
        assert abs(result["p99"].iloc[0] - expected) < 0.01


class TestMultiplePercentiles:
    """Tests for multiple percentiles in single query."""

    def test_all_percentiles_together(self, sample_web_logs):
        """Multiple percentiles in same query."""
        cmd = 'cache=web_logs | stats perc50(response_time) as p50, perc75(response_time) as p75, perc90(response_time) as p90, perc95(response_time) as p95, perc99(response_time) as p99'
        result = CommandExecutor(cmd).execute()

        # Percentiles should be in ascending order
        row = result.iloc[0]
        assert row["p50"] <= row["p75"] <= row["p90"] <= row["p95"] <= row["p99"]

    def test_percentiles_by_group(self, sample_web_logs):
        """Multiple percentiles grouped by field."""
        cmd = 'cache=web_logs | stats perc50(response_time) as p50, perc95(response_time) as p95 by host'
        result = CommandExecutor(cmd).execute()

        # Each row should have p50 <= p95
        for _, row in result.iterrows():
            assert row["p50"] <= row["p95"]

