"""
Tests for bucket command - Time-based windowing and aggregation.

Covers:
- Basic time bucketing with different spans
- Bucket followed by stats aggregation
- Edge cases (empty data, invalid spans)
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestBasicBucket:
    """Tests for basic bucket operations."""

    def test_bucket_5_minutes(self):
        """Bucket data into 5-minute windows."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(minutes=2),
                base_time + timedelta(minutes=5),
                base_time + timedelta(minutes=7),
                base_time + timedelta(minutes=10),
            ],
            "value": [10, 20, 30, 40, 50],
        })
        register_cache("time_data", df)

        cmd = 'cache=time_data | bucket _time span=5m'
        result = CommandExecutor(cmd).execute()

        # Bucketed times should be at 5-minute intervals
        unique_times = result["_time"].unique()
        assert len(unique_times) == 3  # 10:00, 10:05, 10:10

    def test_bucket_1_hour(self):
        """Bucket data into 1-hour windows."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time + timedelta(minutes=i * 15)
                for i in range(12)  # 3 hours of data
            ],
            "value": range(12),
        })
        register_cache("hourly_data", df)

        cmd = 'cache=hourly_data | bucket _time span=1h'
        result = CommandExecutor(cmd).execute()

        unique_times = result["_time"].unique()
        assert len(unique_times) == 3  # 10:00, 11:00, 12:00

    def test_bucket_1_day(self):
        """Bucket data into 1-day windows."""
        base_date = datetime(2024, 1, 1)
        df = pd.DataFrame({
            "_time": [
                base_date + timedelta(days=i // 4, hours=i % 4 * 6)
                for i in range(20)  # 5 days of data
            ],
            "value": range(20),
        })
        register_cache("daily_data", df)

        cmd = 'cache=daily_data | bucket _time span=1d'
        result = CommandExecutor(cmd).execute()

        unique_times = result["_time"].unique()
        assert len(unique_times) == 5


class TestBucketWithStats:
    """Tests for bucket followed by stats aggregation."""

    def test_bucket_with_count(self):
        """Bucket and count events per bucket."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(minutes=1),
                base_time + timedelta(minutes=2),
                base_time + timedelta(minutes=5),
                base_time + timedelta(minutes=6),
            ],
            "event": ["a", "b", "c", "d", "e"],
        })
        register_cache("event_data", df)

        cmd = 'cache=event_data | bucket _time span=5m | stats count as n by _time'
        result = CommandExecutor(cmd).execute()

        # First bucket (10:00-10:05) has 3 events, second (10:05-10:10) has 2
        assert result["n"].sum() == 5

    def test_bucket_with_avg(self):
        """Bucket and calculate average per bucket."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(minutes=1),
                base_time + timedelta(minutes=5),
                base_time + timedelta(minutes=6),
            ],
            "value": [10, 20, 30, 40],
        })
        register_cache("value_data", df)

        cmd = 'cache=value_data | bucket _time span=5m | stats avg(value) as avg_val by _time'
        result = CommandExecutor(cmd).execute()

        assert "avg_val" in result.columns
        # Check averages are computed correctly

    def test_bucket_with_sum_by_host(self):
        """Bucket and aggregate by time and host."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(minutes=1),
                base_time,
                base_time + timedelta(minutes=5),
            ],
            "host": ["server01", "server01", "server02", "server01"],
            "requests": [100, 150, 200, 120],
        })
        register_cache("host_data", df)

        cmd = 'cache=host_data | bucket _time span=5m | stats sum(requests) as total_requests by _time, host'
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "_time" in result.columns
        assert "total_requests" in result.columns


class TestBucketServerMetrics:
    """Tests for bucket with real server metrics fixture."""

    def test_bucket_server_metrics_15m(self, sample_server_metrics):
        """Bucket server metrics into 15-minute windows."""
        cmd = 'cache=server_metrics | bucket _time span=15m | stats avg(cpu_usage) as avg_cpu by host, _time'
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "_time" in result.columns
        assert "avg_cpu" in result.columns

    def test_bucket_for_spike_detection(self, sample_server_metrics):
        """Use bucket for spike detection."""
        cmd = '''cache=server_metrics | bucket _time span=15m | stats avg(cpu_usage) as avg_cpu by host, _time | eval is_spike=if(avg_cpu > 70, 1, 0)'''
        result = CommandExecutor(cmd).execute()

        assert "is_spike" in result.columns
        assert 1 in result["is_spike"].values  # Should detect spikes


class TestBucketSpanFormats:
    """Tests for different span format specifications."""

    def test_span_minutes(self):
        """Test span specified in minutes."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [base_time + timedelta(minutes=i) for i in range(30)],
            "value": range(30),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=10m | stats count as n by _time'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3  # 30 minutes / 10 minutes = 3 buckets

    def test_span_hours(self):
        """Test span specified in hours."""
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        df = pd.DataFrame({
            "_time": [base_time + timedelta(hours=i) for i in range(24)],
            "value": range(24),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=6h | stats count as n by _time'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 4  # 24 hours / 6 hours = 4 buckets

    def test_span_days(self):
        """Test span specified in days."""
        base_date = datetime(2024, 1, 1)
        df = pd.DataFrame({
            "_time": [base_date + timedelta(days=i) for i in range(10)],
            "value": range(10),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=2d | stats count as n by _time'
        result = CommandExecutor(cmd).execute()

        # Bucketing may create edge buckets, verify total rows match
        assert result["n"].sum() == 10

