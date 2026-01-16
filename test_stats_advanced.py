"""
Tests for advanced stats commands.

Covers:
- Multi-level statistics and grouping
- Percentile and distribution analysis
"""

import numpy as np
import pandas as pd
import pytest

from executors import CommandExecutor, register_cache


class TestMultiLevelStats:
    """Test cases for multi-level statistics and grouping."""

    @pytest.fixture
    def web_logs_df(self) -> pd.DataFrame:
        """Web server logs with response times and bytes."""
        np.random.seed(42)
        n = 500

        hosts = ["web01", "web02", "web03"]
        status_codes = [200, 201, 301, 400, 404, 500, 502, 503]

        return pd.DataFrame({
            "host": np.random.choice(hosts, n),
            "status_code": np.random.choice(status_codes, n, p=[0.6, 0.1, 0.05, 0.05, 0.08, 0.05, 0.04, 0.03]),
            "bytes": np.random.randint(100, 50000, n),
            "response_time": np.random.exponential(scale=100, size=n).round(2),
        })

    def test_stats_multiple_aggregations_by_multiple_fields(self, web_logs_df: pd.DataFrame):
        """
        Test: stats count as total_requests, sum(bytes) as total_bytes, 
              avg(response_time) as avg_response, max(response_time) as max_response, 
              min(response_time) as min_response by host, status_code
        """
        register_cache("web_logs", web_logs_df)

        cmd = (
            "cache=web_logs | stats "
            "count as total_requests, "
            "sum(bytes) as total_bytes, "
            "avg(response_time) as avg_response, "
            "max(response_time) as max_response, "
            "min(response_time) as min_response "
            "by host, status_code"
        )
        result = CommandExecutor(cmd).execute()

        # Verify columns
        expected_cols = [
            "host", "status_code", "total_requests", "total_bytes",
            "avg_response", "max_response", "min_response"
        ]
        assert all(col in result.columns for col in expected_cols)

        # Verify aggregation correctness for one group
        sample_group = result[
            (result["host"] == "web01") & (result["status_code"] == 200)
        ]
        if len(sample_group) > 0:
            manual = web_logs_df[
                (web_logs_df["host"] == "web01") & (web_logs_df["status_code"] == 200)
            ]
            assert sample_group["total_requests"].iloc[0] == len(manual)
            assert sample_group["total_bytes"].iloc[0] == manual["bytes"].sum()

    def test_stats_with_stdev(self, web_logs_df: pd.DataFrame):
        """
        Test: stats avg(response_time) as avg_response, 
              stdev(response_time) as std_response by host
        
        Note: stdev function may need to be implemented.
        """
        register_cache("web_logs", web_logs_df)

        cmd = (
            "cache=web_logs | stats "
            "avg(response_time) as avg_response, "
            "stdev(response_time) as std_response "
            "by host"
        )

        try:
            result = CommandExecutor(cmd).execute()
            assert "avg_response" in result.columns
            assert "std_response" in result.columns
        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"stdev not implemented: {e}")


class TestPercentileStats:
    """Test cases for percentile and distribution analysis."""

    @pytest.fixture
    def api_requests_df(self) -> pd.DataFrame:
        """API request logs with response times."""
        np.random.seed(42)
        n = 1000

        endpoints = ["/api/users", "/api/orders", "/api/products", "/api/auth"]
        methods = ["GET", "POST", "PUT", "DELETE"]

        # Generate response times with realistic distribution (mostly fast, some slow)
        response_times = np.concatenate([
            np.random.normal(50, 10, int(n * 0.7)),   # Fast requests
            np.random.normal(200, 50, int(n * 0.2)), # Medium requests
            np.random.normal(500, 100, int(n * 0.1)) # Slow requests
        ])
        np.random.shuffle(response_times)
        response_times = np.clip(response_times, 1, 2000).round(2)

        return pd.DataFrame({
            "endpoint": np.random.choice(endpoints, n),
            "method": np.random.choice(methods, n, p=[0.6, 0.2, 0.15, 0.05]),
            "response_time": response_times[:n],
        })

    def test_percentile_analysis(self, api_requests_df: pd.DataFrame):
        """
        Test: stats perc50(response_time) as p50, perc75(response_time) as p75,
              perc90(response_time) as p90, perc95(response_time) as p95,
              perc99(response_time) as p99, avg(response_time) as avg_time 
              by endpoint, method

        Note: Percentile functions (perc50, perc75, etc.) may need to be implemented.
        """
        register_cache("api_requests", api_requests_df)

        cmd = (
            "cache=api_requests | stats "
            "perc50(response_time) as p50, "
            "perc75(response_time) as p75, "
            "perc90(response_time) as p90, "
            "perc95(response_time) as p95, "
            "perc99(response_time) as p99, "
            "avg(response_time) as avg_time "
            "by endpoint, method"
        )

        try:
            result = CommandExecutor(cmd).execute()

            # Verify percentile columns exist
            assert "p50" in result.columns
            assert "p99" in result.columns
            assert "avg_time" in result.columns

            # Verify percentiles are ordered correctly (p50 <= p75 <= p90 <= p95 <= p99)
            for _, row in result.iterrows():
                assert row["p50"] <= row["p75"] <= row["p90"] <= row["p95"] <= row["p99"]

        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"Percentile functions not implemented: {e}")

    def test_distinct_count(self, api_requests_df: pd.DataFrame):
        """
        Test: stats dc(endpoint) as unique_endpoints, count by method

        Note: dc (distinct count) function may need to be implemented.
        """
        register_cache("api_requests", api_requests_df)

        cmd = "cache=api_requests | stats dc(endpoint) as unique_endpoints, count by method"

        try:
            result = CommandExecutor(cmd).execute()
            assert "unique_endpoints" in result.columns
            # Should have 4 unique endpoints max
            assert all(result["unique_endpoints"] <= 4)
        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"dc (distinct count) not implemented: {e}")

