"""
Tests for time analysis commands - Time range, transaction, and bucket operations.

Covers:
- Transaction command for session analysis
- Bucket command for time windowing
- Time range queries
- Sliding window analysis
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestTransactionCommand:
    """Tests for transaction command - session/event correlation."""

    def test_transaction_basic(self, sample_user_events):
        """
        Test basic transaction grouping.
        
        transaction user_id maxspan=5m
        """
        cmd = 'cache=user_events | transaction user_id maxspan=5m'
        result = CommandExecutor(cmd).execute()
        
        # Should have transaction fields
        assert "user_id" in result.columns
        assert "duration" in result.columns or "_time" in result.columns
        
        # Should have fewer rows than original (grouped by sessions)
        assert len(result) <= len(sample_user_events)

    def test_transaction_with_stats(self, sample_user_events):
        """
        Test transaction followed by stats.
        
        transaction user_id maxspan=5m | stats count as session_count, avg(duration) as avg_duration, 
        sum(event_count) as total_events by user_id | where session_count > 5
        """
        cmd = '''cache=user_events | transaction user_id maxspan=5m | stats count as session_count, avg(duration) as avg_duration by user_id'''
        result = CommandExecutor(cmd).execute()
        
        assert "user_id" in result.columns
        assert "session_count" in result.columns
        assert "avg_duration" in result.columns

    def test_transaction_with_filter(self, sample_user_events):
        """
        Test transaction with filtering on session properties.
        
        transaction user_id maxspan=5m | where session_count > 5
        """
        cmd = '''cache=user_events | transaction user_id maxspan=5m | stats count as session_count by user_id | where session_count > 1'''
        result = CommandExecutor(cmd).execute()
        
        # All rows should have session_count > 1
        if len(result) > 0:
            assert all(result["session_count"] > 1)

    def test_transaction_maxspan_variations(self, sample_user_events):
        """Test transaction with different maxspan values."""
        # Shorter maxspan should create more transactions
        cmd_short = 'cache=user_events | transaction user_id maxspan=1m | stats count as sessions by user_id'
        result_short = CommandExecutor(cmd_short).execute()
        
        cmd_long = 'cache=user_events | transaction user_id maxspan=30m | stats count as sessions by user_id'
        result_long = CommandExecutor(cmd_long).execute()
        
        # Both should have results
        assert len(result_short) > 0
        assert len(result_long) > 0


class TestBucketCommand:
    """Tests for bucket command - time windowing."""

    def test_bucket_by_time(self, sample_server_metrics):
        """
        Test basic time bucketing.
        
        bucket _time span=5m
        """
        cmd = 'cache=server_metrics | bucket _time span=5m'
        result = CommandExecutor(cmd).execute()
        
        # Should have same row count
        assert len(result) == len(sample_server_metrics)
        
        # _time should be bucketed
        assert "_time" in result.columns

    def test_bucket_with_stats(self, sample_server_metrics):
        """
        Test bucket followed by stats aggregation.
        
        bucket _time span=5m | stats avg(cpu) as avg_cpu, avg(memory) as avg_mem, count by host, _time
        """
        cmd = 'cache=server_metrics | bucket _time span=5m | stats avg(cpu_usage) as avg_cpu, avg(memory_usage) as avg_mem, count by host, _time'
        result = CommandExecutor(cmd).execute()
        
        assert "host" in result.columns
        assert "_time" in result.columns
        assert "avg_cpu" in result.columns
        assert "avg_mem" in result.columns
        assert "count" in result.columns
        
        # Should have multiple time buckets
        assert len(result["_time"].unique()) > 1

    def test_bucket_span_1h(self, sample_server_metrics):
        """Test hourly bucketing."""
        cmd = 'cache=server_metrics | bucket _time span=1h | stats avg(cpu_usage) as avg_cpu by _time'
        result = CommandExecutor(cmd).execute()
        
        assert "_time" in result.columns
        assert "avg_cpu" in result.columns

    def test_bucket_span_1d(self):
        """Test daily bucketing."""
        # Create data spanning multiple days
        base_time = datetime(2024, 1, 1)
        df = pd.DataFrame({
            "_time": [base_time + timedelta(days=i, hours=np.random.randint(0, 24)) 
                     for i in range(30)],
            "value": np.random.randint(1, 100, 30),
        })
        register_cache("daily_data", df)
        
        cmd = 'cache=daily_data | bucket _time span=1d | stats sum(value) as daily_total by _time'
        result = CommandExecutor(cmd).execute()
        
        # Should have <= 30 daily buckets
        assert len(result) <= 30
        assert "daily_total" in result.columns


class TestSlidingWindowAnalysis:
    """Tests for sliding window time analysis."""

    def test_time_window_aggregation(self, sample_server_metrics):
        """
        Test time window sliding analysis.
        
        bucket _time span=5m | stats avg(cpu) as avg_cpu, avg(memory) as avg_mem, count by host, _time
        """
        cmd = 'cache=server_metrics | bucket _time span=5m | stats avg(cpu_usage) as avg_cpu, avg(memory_usage) as avg_mem, count as n by host, _time'
        result = CommandExecutor(cmd).execute()
        
        # Should be grouped by both host and time bucket
        assert len(result["host"].unique()) == 3  # web01, web02, web03
        
        # Each host should have multiple time buckets
        for host in result["host"].unique():
            host_data = result[result["host"] == host]
            assert len(host_data) > 1

    def test_rolling_average_simulation(self, sample_server_metrics):
        """
        Simulate rolling average with bucket and stats.
        """
        cmd = 'cache=server_metrics | bucket _time span=15m | stats avg(cpu_usage) as avg_cpu by host, _time'
        result = CommandExecutor(cmd).execute()
        
        # Verify we have time-series data
        assert "_time" in result.columns
        assert "avg_cpu" in result.columns
        
        # Values should be reasonable (0-100 for CPU)
        assert all(0 <= v <= 100 for v in result["avg_cpu"])


class TestTimeRangeQueries:
    """Tests for time range based queries."""

    def test_latest_time_range(self):
        """
        Test latest=-5m style time range.
        
        search latest=-5m
        """
        # Create recent data
        now = datetime.now()
        df = pd.DataFrame({
            "_time": [now - timedelta(minutes=i) for i in range(10)],
            "value": range(10),
        })
        register_cache("recent_data", df)
        
        cmd = 'cache=recent_data | search latest=-5m'
        result = CommandExecutor(cmd).execute()
        
        # Should filter to last 5 minutes
        if len(result) > 0:
            min_time = now - timedelta(minutes=5)
            assert all(result["_time"] >= min_time)

    def test_earliest_time_range(self):
        """
        Test earliest time range filter.
        """
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [base_time + timedelta(hours=i) for i in range(24)],
            "value": range(24),
        })
        register_cache("hourly_data", df)
        
        cmd = 'cache=hourly_data | search earliest="2024-01-01 15:00:00"'
        result = CommandExecutor(cmd).execute()
        
        # Should only include data from 15:00 onwards
        if len(result) > 0:
            earliest = datetime(2024, 1, 1, 15, 0, 0)
            assert all(result["_time"] >= earliest)

    def test_time_range_between(self):
        """Test time range between two dates."""
        base_time = datetime(2024, 1, 1)
        df = pd.DataFrame({
            "_time": [base_time + timedelta(days=i) for i in range(30)],
            "value": range(30),
        })
        register_cache("monthly_data", df)
        
        cmd = 'cache=monthly_data | search earliest="2024-01-10" latest="2024-01-20"'
        result = CommandExecutor(cmd).execute()
        
        # Should only include data between Jan 10-20
        if len(result) > 0:
            start = datetime(2024, 1, 10)
            end = datetime(2024, 1, 20)
            assert all((result["_time"] >= start) & (result["_time"] <= end))


class TestTimeBasedCorrelation:
    """Tests for time-based event correlation."""

    def test_time_correlation_with_join(self):
        """
        Test correlating events from different sources by time.
        """
        # Create two event streams
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        requests = pd.DataFrame({
            "_time": [base_time + timedelta(seconds=i * 10) for i in range(20)],
            "request_id": range(20),
            "endpoint": ["/api/v1", "/api/v2"] * 10,
        })
        register_cache("requests", requests)
        
        responses = pd.DataFrame({
            "_time": [base_time + timedelta(seconds=i * 10 + 5) for i in range(20)],
            "request_id": range(20),
            "status": [200, 404] * 10,
        })
        register_cache("responses", responses)
        
        # Join on request_id
        cmd = 'cache=requests | join request_id [search index="responses" | stats first(status) as status by request_id]'
        result = CommandExecutor(cmd).execute()
        
        assert "status" in result.columns
        assert len(result) == 20


class TestCompleteTimeAnalysisPipelines:
    """Integration tests for complete time analysis pipelines."""

    def test_hourly_performance_analysis(self, sample_server_metrics):
        """
        Complete hourly performance analysis.
        
        eval hour=strftime(_time, "%H") | stats avg(cpu_usage) as avg_cpu, max(memory_usage) as max_mem by host, hour
        """
        cmd = '''cache=server_metrics | eval hour=strftime(_time, "%H") | stats avg(cpu_usage) as avg_cpu, max(memory_usage) as max_mem by host, hour'''
        result = CommandExecutor(cmd).execute()
        
        assert "host" in result.columns
        assert "hour" in result.columns
        assert "avg_cpu" in result.columns
        assert "max_mem" in result.columns

    def test_session_analysis_pipeline(self, sample_user_events):
        """
        Complete session analysis pipeline.
        
        transaction user_id maxspan=5m | stats count as session_count, avg(duration) as avg_duration,
        sum(event_count) as total_events by user_id | where session_count > 5
        """
        cmd = '''cache=user_events | transaction user_id maxspan=5m | stats count as session_count by user_id | where session_count > 1'''
        result = CommandExecutor(cmd).execute()
        
        if len(result) > 0:
            assert all(result["session_count"] > 1)

    def test_time_bucket_trend_analysis(self, sample_server_metrics):
        """
        Time bucket trend analysis.
        
        bucket _time span=5m | stats avg(cpu) as avg_cpu by host, _time | 
        eval trend=if(avg_cpu>50, "high", "normal")
        """
        cmd = '''cache=server_metrics | bucket _time span=5m | stats avg(cpu_usage) as avg_cpu by host, _time | eval trend=if(avg_cpu>50, "high", "normal")'''
        result = CommandExecutor(cmd).execute()
        
        assert "trend" in result.columns
        valid_trends = {"high", "normal"}
        assert set(result["trend"].unique()).issubset(valid_trends)


class TestBucketSpanFormats:
    """Tests for different bucket span format specifications."""

    def test_bucket_seconds(self, sample_server_metrics):
        """Test bucket with seconds span."""
        cmd = 'cache=server_metrics | bucket _time span=30s | stats count by _time'
        result = CommandExecutor(cmd).execute()
        
        assert "_time" in result.columns
        assert len(result) > 0

    def test_bucket_minutes(self, sample_server_metrics):
        """Test bucket with minutes span."""
        cmd = 'cache=server_metrics | bucket _time span=10m | stats count by _time'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) > 0

    def test_bucket_hours(self):
        """Test bucket with hours span."""
        base_time = datetime(2024, 1, 1)
        df = pd.DataFrame({
            "_time": [base_time + timedelta(hours=i) for i in range(48)],
            "value": range(48),
        })
        register_cache("hourly_test", df)
        
        cmd = 'cache=hourly_test | bucket _time span=6h | stats sum(value) as total by _time'
        result = CommandExecutor(cmd).execute()
        
        # 48 hours / 6 hour buckets = 8 buckets
        assert len(result) <= 8

    def test_bucket_days(self):
        """Test bucket with days span."""
        base_time = datetime(2024, 1, 1)
        df = pd.DataFrame({
            "_time": [base_time + timedelta(days=i) for i in range(30)],
            "value": range(30),
        })
        register_cache("daily_test", df)
        
        cmd = 'cache=daily_test | bucket _time span=7d | stats sum(value) as weekly_total by _time'
        result = CommandExecutor(cmd).execute()
        
        # 30 days / 7 day buckets = ~5 buckets
        assert len(result) <= 5

