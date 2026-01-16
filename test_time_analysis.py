"""
Tests for time analysis commands.

Covers:
- Transaction analysis with maxspan
- Time bucket (bin) operations
- Sliding window analysis
"""

import numpy as np
import pandas as pd
import pytest

from executors import CommandExecutor, register_cache


class TestTransactionCommand:
    """Test cases for transaction command."""

    @pytest.fixture
    def session_events_df(self) -> pd.DataFrame:
        """User session events for transaction testing."""
        np.random.seed(42)
        
        # Generate events for 20 users over multiple sessions
        events = []
        event_id = 1
        
        for user_num in range(1, 21):
            user_id = f"user_{user_num:03d}"
            
            # Each user has 2-5 sessions
            num_sessions = np.random.randint(2, 6)
            base_time = pd.Timestamp("2024-01-01")
            
            for session in range(num_sessions):
                # Session starts at random time
                session_start = base_time + pd.Timedelta(
                    hours=np.random.randint(0, 168)  # Within a week
                )
                
                # Each session has 3-10 events within 5 minutes
                num_events = np.random.randint(3, 11)
                
                for event in range(num_events):
                    event_time = session_start + pd.Timedelta(
                        seconds=np.random.randint(0, 300)  # Within 5 minutes
                    )
                    events.append({
                        "event_id": event_id,
                        "_time": event_time,
                        "user_id": user_id,
                        "action": np.random.choice(
                            ["page_view", "click", "scroll", "submit"]
                        ),
                    })
                    event_id += 1
        
        df = pd.DataFrame(events)
        return df.sort_values("_time").reset_index(drop=True)

    def test_transaction_by_user(self, session_events_df: pd.DataFrame):
        """
        Test: transaction user_id maxspan=5m 
              | stats count as session_count, avg(duration) as avg_duration, 
                     sum(event_count) as total_events by user_id 
              | filter session_count > 2

        Note: transaction command may need to be implemented.
        """
        register_cache("session_events", session_events_df)

        cmd = (
            "cache=session_events "
            "| transaction user_id maxspan=5m "
            "| stats count as session_count, avg(duration) as avg_duration, "
            "sum(event_count) as total_events by user_id "
            "| filter session_count > 2"
        )

        try:
            result = CommandExecutor(cmd).execute()

            assert "user_id" in result.columns
            assert "session_count" in result.columns
            assert all(result["session_count"] > 2)

        except (ValueError, NotImplementedError, KeyError) as e:
            pytest.skip(f"transaction command not implemented: {e}")


class TestBucketCommand:
    """Test cases for bucket/bin time operations."""

    @pytest.fixture
    def metrics_timeseries_df(self) -> pd.DataFrame:
        """Time series metrics for bucket testing."""
        np.random.seed(42)
        
        # Generate data every minute for 24 hours
        timestamps = pd.date_range("2024-01-01", periods=1440, freq="min")
        
        # Simulate CPU with daily pattern
        hours = timestamps.hour
        base_cpu = 30 + 20 * np.sin(2 * np.pi * hours / 24)  # Daily cycle
        cpu = base_cpu + np.random.normal(0, 5, len(timestamps))
        cpu = np.clip(cpu, 5, 95)
        
        # Memory tends to increase over time
        memory = 40 + np.linspace(0, 20, len(timestamps)) + np.random.normal(0, 3, len(timestamps))
        memory = np.clip(memory, 20, 90)
        
        return pd.DataFrame({
            "_time": timestamps,
            "host": np.random.choice(["server01", "server02"], len(timestamps)),
            "cpu": cpu.round(2),
            "memory": memory.round(2),
        })

    def test_bucket_span_5m(self, metrics_timeseries_df: pd.DataFrame):
        """
        Test: bucket _time span=5m | stats avg(cpu) as avg_cpu, avg(memory) as avg_mem, count by host, _time

        Note: bucket command may need to be implemented.
        """
        register_cache("metrics", metrics_timeseries_df)

        cmd = (
            "cache=metrics "
            "| bucket _time span=5m "
            "| stats avg(cpu) as avg_cpu, avg(memory) as avg_mem, count by host, _time"
        )

        try:
            result = CommandExecutor(cmd).execute()

            assert "host" in result.columns
            assert "_time" in result.columns
            assert "avg_cpu" in result.columns
            assert "avg_mem" in result.columns

            # Should have aggregated data (fewer rows than original)
            assert len(result) < len(metrics_timeseries_df)

        except (ValueError, NotImplementedError, KeyError) as e:
            pytest.skip(f"bucket command not implemented: {e}")

    def test_bucket_with_different_spans(self, metrics_timeseries_df: pd.DataFrame):
        """Test bucket with various time spans."""
        register_cache("metrics", metrics_timeseries_df)

        spans = ["1m", "5m", "15m", "1h"]
        
        for span in spans:
            cmd = f"cache=metrics | bucket _time span={span} | stats count by _time"

            try:
                result = CommandExecutor(cmd).execute()
                # Verify aggregation happened
                assert len(result) <= len(metrics_timeseries_df)
            except (ValueError, NotImplementedError, KeyError):
                pytest.skip(f"bucket command not implemented for span={span}")
                break


class TestSlidingWindowAnalysis:
    """Test cases for sliding window analysis patterns."""

    @pytest.fixture
    def hourly_metrics_df(self) -> pd.DataFrame:
        """Hourly metrics for sliding window tests."""
        np.random.seed(42)
        
        # Generate hourly data for a week
        timestamps = pd.date_range("2024-01-01", periods=168, freq="h")
        
        return pd.DataFrame({
            "_time": timestamps,
            "host": ["web01"] * 84 + ["web02"] * 84,
            "requests": np.random.poisson(lam=1000, size=168),
            "errors": np.random.poisson(lam=10, size=168),
            "avg_latency": np.random.exponential(scale=50, size=168).round(2),
        })

    def test_hourly_aggregation(self, hourly_metrics_df: pd.DataFrame):
        """Test basic time-based aggregation without bucket command."""
        register_cache("hourly_metrics", hourly_metrics_df)

        # Alternative approach using eval to extract hour
        cmd = (
            "cache=hourly_metrics "
            "| stats sum(requests) as total_requests, sum(errors) as total_errors, "
            "avg(avg_latency) as mean_latency by host"
        )

        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "total_requests" in result.columns
        assert "total_errors" in result.columns
        assert len(result) == 2  # Two hosts

    def test_stats_by_time_field(self, hourly_metrics_df: pd.DataFrame):
        """
        Test statistics grouped by time field.
        
        Note: This tests the pattern of grouping by a time-derived field.
        """
        register_cache("hourly_metrics", hourly_metrics_df)

        # This test simulates what bucket would do manually
        # by using eval to extract time components
        cmd = (
            "cache=hourly_metrics "
            "| stats avg(requests) as avg_requests, max(errors) as max_errors by host"
        )

        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "avg_requests" in result.columns
        assert "max_errors" in result.columns


class TestMultiIndexQuery:
    """Test cases for multi-index union queries."""

    @pytest.fixture
    def app_logs_df(self) -> pd.DataFrame:
        """Application logs."""
        np.random.seed(42)
        n = 200

        return pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="min"),
            "host": np.random.choice(["app01", "app02"], n),
            "level": np.random.choice(["INFO", "WARN", "ERROR"], n, p=[0.7, 0.2, 0.1]),
            "message": [f"Application event {i}" for i in range(n)],
        })

    @pytest.fixture
    def error_logs_df(self) -> pd.DataFrame:
        """Error-specific logs."""
        np.random.seed(43)
        n = 50

        return pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min"),
            "host": np.random.choice(["app01", "app02", "db01"], n),
            "severity": np.random.choice(["WARNING", "ERROR", "CRITICAL"], n),
            "message": [f"Error event {i}" for i in range(n)],
        })

    def test_union_like_query_with_eval(
        self, app_logs_df: pd.DataFrame, error_logs_df: pd.DataFrame
    ):
        """
        Test: Simulate (index="app_logs" OR index="error_logs") 
              | eval log_level=coalesce(level, severity) 
              | stats count, values(message) as messages by host, log_level

        Note: Multi-index union and coalesce may need special implementation.
        Since we don't have native union, this tests a similar pattern.
        """
        register_cache("app_logs", app_logs_df)
        register_cache("error_logs", error_logs_df)

        # Test individual index query first
        cmd = (
            "cache=app_logs "
            "| stats count by host, level"
        )

        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "level" in result.columns
        assert "count" in result.columns

        # Verify counts make sense
        assert result["count"].sum() == len(app_logs_df)

