"""
Tests for bucket command - Time binning.

Group events into time-based buckets
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestBasicTimeBinning:
    """Tests for basic time binning."""

    def test_bucket_5_minute(self, sample_server_metrics):
        """Bucket into 5-minute intervals."""
        cmd = 'cache=server_metrics | bucket _time span=5m'
        result = CommandExecutor(cmd).execute()

        assert "_time" in result.columns
        # Check that bucketed times are multiples of 5 minutes
        # (implementation-dependent)

    def test_bucket_hourly(self, sample_server_metrics):
        """Bucket into hourly intervals."""
        cmd = 'cache=server_metrics | bucket _time span=1h'
        result = CommandExecutor(cmd).execute()

        assert "_time" in result.columns

    def test_bucket_creates_time_field(self):
        """Bucket creates proper time field."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=12, freq="5min"),
            "value": range(12),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=15m'
        result = CommandExecutor(cmd).execute()

        assert "_time" in result.columns
        # Should have 4 buckets (60 minutes / 15 minute span)
        assert result["_time"].nunique() == 4


class TestBucketWithStats:
    """Tests for bucket combined with stats."""

    def test_bucket_then_stats(self, sample_server_metrics):
        """Bucket followed by stats aggregation."""
        cmd = 'cache=server_metrics | bucket _time span=5m | stats avg(cpu_usage) as avg_cpu by _time'
        result = CommandExecutor(cmd).execute()

        assert "avg_cpu" in result.columns
        assert "_time" in result.columns

    def test_bucket_count_per_interval(self):
        """Count events per time bucket."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=20, freq="2min"),
            "event": range(20),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=10m | stats count as events by _time'
        result = CommandExecutor(cmd).execute()

        assert "events" in result.columns


class TestBucketGroupBy:
    """Tests for bucket with multiple dimensions."""

    def test_bucket_by_host(self, sample_server_metrics):
        """Bucket and group by host."""
        cmd = 'cache=server_metrics | bucket _time span=5m | stats avg(cpu_usage) as avg_cpu by _time, host'
        result = CommandExecutor(cmd).execute()

        assert "_time" in result.columns
        assert "host" in result.columns
        assert "avg_cpu" in result.columns

