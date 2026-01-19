"""
Tests for transaction command - Maxspan parameter.

Control maximum duration of transactions
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestMaxspanBasic:
    """Tests for basic maxspan functionality."""

    def test_maxspan_30_seconds(self):
        """Maxspan of 30 seconds."""
        df = pd.DataFrame({
            "_time": [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 10, 0, 20),
                datetime(2024, 1, 1, 10, 1, 0),  # More than 30s from first event
                datetime(2024, 1, 1, 10, 1, 15),
            ],
            "user": ["A", "A", "A", "A"],
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user maxspan=30s'
        result = CommandExecutor(cmd).execute()

        # Should split into multiple transactions due to maxspan
        assert len(result) >= 2

    def test_maxspan_5_minutes(self, sample_session_logs):
        """Maxspan of 5 minutes - check transactions have duration constraint."""
        cmd = 'cache=session_logs | transaction session_id maxspan=5m'
        result = CommandExecutor(cmd).execute()

        # Transactions should be created
        assert len(result) > 0
        assert "duration" in result.columns


class TestMaxspanFormats:
    """Tests for different maxspan format strings."""

    def test_maxspan_seconds(self):
        """Maxspan in seconds."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=10, freq="20s"),
            "user": ["A"] * 10,
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user maxspan=60s'
        result = CommandExecutor(cmd).execute()

        assert len(result) > 0

    def test_maxspan_minutes(self):
        """Maxspan in minutes."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 10:00:00", periods=20, freq="1min"),
            "user": ["A"] * 20,
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user maxspan=5m'
        result = CommandExecutor(cmd).execute()

        # Transactions should be created
        assert len(result) >= 1

    def test_maxspan_hours(self):
        """Maxspan in hours."""
        df = pd.DataFrame({
            "_time": pd.date_range("2024-01-01 00:00:00", periods=10, freq="30min"),
            "user": ["A"] * 10,
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user maxspan=1h'
        result = CommandExecutor(cmd).execute()

        # Transactions should be created
        assert len(result) >= 1

