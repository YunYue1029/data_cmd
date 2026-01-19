"""
Tests for eval command - Time/Date functions.

Functions tested: strftime, strptime, year, month, day, hour, minute, second
"""

import pytest
import pandas as pd
from datetime import datetime

from RDP.executors import CommandExecutor, register_cache


class TestStrftime:
    """Tests for strftime() function - format datetime to string."""

    def test_strftime_hour(self, sample_server_metrics):
        """Extract hour using strftime."""
        cmd = 'cache=server_metrics | eval hour=strftime(_time, "%H")'
        result = CommandExecutor(cmd).execute()

        assert "hour" in result.columns
        # Hours should be 2-digit strings
        assert all(len(str(h)) <= 2 for h in result["hour"].unique())

    def test_strftime_date(self, sample_orders):
        """Format date using strftime."""
        cmd = 'cache=orders | eval date_str=strftime(order_date, "%Y-%m-%d")'
        result = CommandExecutor(cmd).execute()

        assert "date_str" in result.columns
        # Should be in YYYY-MM-DD format
        assert all("-" in str(d) for d in result["date_str"])

    def test_strftime_full_datetime(self):
        """Format full datetime."""
        df = pd.DataFrame({
            "_time": [datetime(2024, 1, 15, 10, 30, 45)]
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval formatted=strftime(_time, "%Y-%m-%d %H:%M:%S")'
        result = CommandExecutor(cmd).execute()

        assert result["formatted"].iloc[0] == "2024-01-15 10:30:45"


class TestStrptime:
    """Tests for strptime() function - parse string to datetime."""

    def test_strptime_basic(self):
        """Parse datetime string."""
        df = pd.DataFrame({
            "timestamp": ["2024-01-01 10:00:00", "2024-01-02 15:30:00"]
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval parsed=strptime(timestamp, "%Y-%m-%d %H:%M:%S")'
        result = CommandExecutor(cmd).execute()

        assert "parsed" in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["parsed"])

    def test_strptime_date_only(self):
        """Parse date-only string."""
        df = pd.DataFrame({
            "date_str": ["2024-01-01", "2024-06-15"]
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval parsed=strptime(date_str, "%Y-%m-%d")'
        result = CommandExecutor(cmd).execute()

        assert pd.api.types.is_datetime64_any_dtype(result["parsed"])


class TestYear:
    """Tests for year() function."""

    def test_year_extraction(self, sample_orders):
        """Extract year from date."""
        cmd = 'cache=orders | eval year=year(order_date)'
        result = CommandExecutor(cmd).execute()

        assert "year" in result.columns
        assert 2024 in result["year"].unique()


class TestMonth:
    """Tests for month() function."""

    def test_month_extraction(self, sample_orders):
        """Extract month from date."""
        cmd = 'cache=orders | eval month=month(order_date)'
        result = CommandExecutor(cmd).execute()

        assert "month" in result.columns
        # Months should be 1-12
        assert all(1 <= m <= 12 for m in result["month"])


class TestDay:
    """Tests for day() function."""

    def test_day_extraction(self, sample_orders):
        """Extract day from date."""
        cmd = 'cache=orders | eval day=day(order_date)'
        result = CommandExecutor(cmd).execute()

        assert "day" in result.columns
        # Days should be 1-31
        assert all(1 <= d <= 31 for d in result["day"])


class TestHourMinuteSecond:
    """Tests for hour(), minute(), second() functions."""

    def test_hour_extraction(self, sample_server_metrics):
        """Extract hour from datetime."""
        cmd = 'cache=server_metrics | eval hour=hour(_time)'
        result = CommandExecutor(cmd).execute()

        assert "hour" in result.columns
        assert all(0 <= h <= 23 for h in result["hour"])

    def test_minute_extraction(self):
        """Extract minute from datetime."""
        df = pd.DataFrame({
            "_time": [datetime(2024, 1, 1, 10, 30, 45)]
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval minute=minute(_time)'
        result = CommandExecutor(cmd).execute()

        assert result["minute"].iloc[0] == 30

    def test_second_extraction(self):
        """Extract second from datetime."""
        df = pd.DataFrame({
            "_time": [datetime(2024, 1, 1, 10, 30, 45)]
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval second=second(_time)'
        result = CommandExecutor(cmd).execute()

        assert result["second"].iloc[0] == 45


class TestCombinedTimeExtraction:
    """Tests for combining multiple time extractions."""

    def test_extract_all_parts(self, sample_orders):
        """Extract year, month, day from date."""
        cmd = 'cache=orders | eval year=year(order_date) | eval month=month(order_date) | eval day=day(order_date)'
        result = CommandExecutor(cmd).execute()

        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns

