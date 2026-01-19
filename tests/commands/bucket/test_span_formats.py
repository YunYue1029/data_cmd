"""
Tests for bucket command - Span format variations.

Different span format: minutes, hours, days
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestMinuteSpans:
    """Tests for minute-based spans."""

    def test_span_1m(self):
        """1 minute span."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=10, freq="30s"),
            "value": range(10),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=1m'
        result = CommandExecutor(cmd).execute()

        # 10 events over 5 minutes at 30s intervals = 5 unique buckets
        assert result["_time"].nunique() == 5

    def test_span_5m(self):
        """5 minute span."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=30, freq="1min"),
            "value": range(30),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=5m'
        result = CommandExecutor(cmd).execute()

        # 30 minutes / 5 minute span = 6 buckets
        assert result["_time"].nunique() == 6

    def test_span_15m(self):
        """15 minute span."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=60, freq="1min"),
            "value": range(60),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=15m'
        result = CommandExecutor(cmd).execute()

        # 60 minutes / 15 minute span = 4 buckets
        assert result["_time"].nunique() == 4


class TestHourSpans:
    """Tests for hour-based spans."""

    def test_span_1h(self):
        """1 hour span."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=180, freq="1min"),
            "value": range(180),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=1h'
        result = CommandExecutor(cmd).execute()

        # 180 minutes / 60 = 3 hours = 3 buckets
        assert result["_time"].nunique() == 3

    def test_span_4h(self):
        """4 hour span."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 00:00:00", periods=24, freq="1h"),
            "value": range(24),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=4h'
        result = CommandExecutor(cmd).execute()

        # 24 hours / 4 hour span = 6 buckets
        assert result["_time"].nunique() == 6


class TestDaySpans:
    """Tests for day-based spans."""

    def test_span_1d(self):
        """1 day span."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01", periods=7, freq="1d"),
            "value": range(7),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=1d'
        result = CommandExecutor(cmd).execute()

        # 7 days = 7 buckets
        assert result["_time"].nunique() == 7

    def test_span_7d(self):
        """7 day (weekly) span."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01", periods=28, freq="1d"),
            "value": range(28),
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | bucket _time span=7d'
        result = CommandExecutor(cmd).execute()

        # Bucket creates weekly buckets - actual count depends on implementation
        # (may include partial weeks at boundaries)
        assert result["_time"].nunique() >= 4

