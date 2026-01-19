"""
Tests for stats command - Statistics and aggregation analysis.

Covers:
- Multi-level statistics with grouping
- Percentile and distribution analysis
- Standard deviation calculations
- Basic aggregation functions (count, sum, avg, min, max)
- Advanced aggregation functions (values, dc, first, last)
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestBasicStatsAggregation:
    """Tests for basic aggregation functions."""

    def test_count_simple(self, sample_web_logs):
        """Test simple count aggregation."""
        cmd = 'cache=web_logs | stats count as total'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "total" in result.columns
        assert result["total"].iloc[0] == len(sample_web_logs)

    def test_count_by_field(self, sample_web_logs):
        """Test count grouped by a single field."""
        cmd = 'cache=web_logs | stats count as total by host'
        result = CommandExecutor(cmd).execute()
        
        # Should have one row per unique host
        expected_hosts = sample_web_logs["host"].nunique()
        assert len(result) == expected_hosts
        assert "host" in result.columns
        assert "total" in result.columns
        
        # Verify counts match
        expected = sample_web_logs.groupby("host").size().reset_index(name="total")
        result_sorted = result.sort_values("host").reset_index(drop=True)
        expected_sorted = expected.sort_values("host").reset_index(drop=True)
        assert result_sorted["total"].sum() == expected_sorted["total"].sum()

    def test_sum_aggregation(self, sample_web_logs):
        """Test sum aggregation."""
        cmd = 'cache=web_logs | stats sum(bytes) as total_bytes'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "total_bytes" in result.columns
        assert result["total_bytes"].iloc[0] == sample_web_logs["bytes"].sum()

    def test_avg_aggregation(self, sample_web_logs):
        """Test average aggregation."""
        cmd = 'cache=web_logs | stats avg(response_time) as avg_response'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "avg_response" in result.columns
        expected_avg = sample_web_logs["response_time"].mean()
        assert abs(result["avg_response"].iloc[0] - expected_avg) < 0.01

    def test_min_max_aggregation(self, sample_web_logs):
        """Test min and max aggregation."""
        cmd = 'cache=web_logs | stats min(response_time) as min_response, max(response_time) as max_response'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "min_response" in result.columns
        assert "max_response" in result.columns
        assert result["min_response"].iloc[0] == sample_web_logs["response_time"].min()
        assert result["max_response"].iloc[0] == sample_web_logs["response_time"].max()


class TestMultiLevelStatsAggregation:
    """Tests for multi-level statistics with grouping."""

    def test_multiple_aggregations_single_group(self, sample_web_logs):
        """
        Test multiple aggregations grouped by single field.
        
        stats count as total_requests, sum(bytes) as total_bytes, avg(response_time) as avg_response by host
        """
        cmd = '''cache=web_logs | stats count as total_requests, sum(bytes) as total_bytes, avg(response_time) as avg_response by host'''
        result = CommandExecutor(cmd).execute()
        
        # Verify structure
        expected_cols = ["host", "total_requests", "total_bytes", "avg_response"]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"
        
        # Verify values for each host
        for host in sample_web_logs["host"].unique():
            host_data = sample_web_logs[sample_web_logs["host"] == host]
            result_row = result[result["host"] == host]
            
            assert len(result_row) == 1
            assert result_row["total_requests"].iloc[0] == len(host_data)
            assert result_row["total_bytes"].iloc[0] == host_data["bytes"].sum()
            assert abs(result_row["avg_response"].iloc[0] - host_data["response_time"].mean()) < 0.01

    def test_multiple_aggregations_multiple_groups(self, sample_web_logs):
        """
        Test multiple aggregations grouped by multiple fields.
        
        stats count as total_requests, sum(bytes) as total_bytes, avg(response_time) as avg_response, 
        max(response_time) as max_response, min(response_time) as min_response by host, status_code
        """
        cmd = '''cache=web_logs | stats count as total_requests, sum(bytes) as total_bytes, avg(response_time) as avg_response, max(response_time) as max_response, min(response_time) as min_response by host, status_code'''
        result = CommandExecutor(cmd).execute()
        
        # Verify structure
        expected_cols = ["host", "status_code", "total_requests", "total_bytes", 
                        "avg_response", "max_response", "min_response"]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"
        
        # Number of groups should match
        expected_groups = sample_web_logs.groupby(["host", "status_code"]).ngroups
        assert len(result) == expected_groups


class TestStandardDeviation:
    """Tests for standard deviation aggregation."""

    def test_stdev_simple(self, sample_web_logs):
        """
        Test standard deviation calculation.
        
        stats stdev(response_time) as std_response
        """
        cmd = 'cache=web_logs | stats stdev(response_time) as std_response'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "std_response" in result.columns
        expected_std = sample_web_logs["response_time"].std()
        assert abs(result["std_response"].iloc[0] - expected_std) < 0.01

    def test_stdev_by_group(self, sample_web_logs):
        """
        Test standard deviation with grouping.
        
        stats avg(response_time) as avg_response, stdev(response_time) as std_response by host
        """
        cmd = 'cache=web_logs | stats avg(response_time) as avg_response, stdev(response_time) as std_response by host'
        result = CommandExecutor(cmd).execute()
        
        assert "avg_response" in result.columns
        assert "std_response" in result.columns
        
        # Verify for each host
        for host in sample_web_logs["host"].unique():
            host_data = sample_web_logs[sample_web_logs["host"] == host]
            result_row = result[result["host"] == host]
            
            expected_std = host_data["response_time"].std()
            assert abs(result_row["std_response"].iloc[0] - expected_std) < 0.01


class TestPercentileAggregation:
    """Tests for percentile and distribution analysis."""

    def test_perc50(self, sample_web_logs):
        """Test 50th percentile (median)."""
        cmd = 'cache=web_logs | stats perc50(response_time) as p50'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "p50" in result.columns
        expected_p50 = sample_web_logs["response_time"].quantile(0.50)
        assert abs(result["p50"].iloc[0] - expected_p50) < 0.01

    def test_perc75(self, sample_web_logs):
        """Test 75th percentile."""
        cmd = 'cache=web_logs | stats perc75(response_time) as p75'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "p75" in result.columns
        expected_p75 = sample_web_logs["response_time"].quantile(0.75)
        assert abs(result["p75"].iloc[0] - expected_p75) < 0.01

    def test_perc90(self, sample_web_logs):
        """Test 90th percentile."""
        cmd = 'cache=web_logs | stats perc90(response_time) as p90'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "p90" in result.columns
        expected_p90 = sample_web_logs["response_time"].quantile(0.90)
        assert abs(result["p90"].iloc[0] - expected_p90) < 0.01

    def test_perc95(self, sample_web_logs):
        """Test 95th percentile."""
        cmd = 'cache=web_logs | stats perc95(response_time) as p95'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "p95" in result.columns
        expected_p95 = sample_web_logs["response_time"].quantile(0.95)
        assert abs(result["p95"].iloc[0] - expected_p95) < 0.01

    def test_perc99(self, sample_web_logs):
        """Test 99th percentile."""
        cmd = 'cache=web_logs | stats perc99(response_time) as p99'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 1
        assert "p99" in result.columns
        expected_p99 = sample_web_logs["response_time"].quantile(0.99)
        assert abs(result["p99"].iloc[0] - expected_p99) < 0.01

    def test_multiple_percentiles_with_avg(self, sample_web_logs):
        """
        Test multiple percentiles with average.
        
        stats perc50(response_time) as p50, perc75(response_time) as p75, 
        perc90(response_time) as p90, perc95(response_time) as p95, 
        perc99(response_time) as p99, avg(response_time) as avg_time by uri, method
        """
        cmd = '''cache=web_logs | stats perc50(response_time) as p50, perc75(response_time) as p75, perc90(response_time) as p90, perc95(response_time) as p95, perc99(response_time) as p99, avg(response_time) as avg_time by uri, method'''
        result = CommandExecutor(cmd).execute()
        
        expected_cols = ["uri", "method", "p50", "p75", "p90", "p95", "p99", "avg_time"]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"
        
        # Verify percentiles are in order
        for _, row in result.iterrows():
            assert row["p50"] <= row["p75"] <= row["p90"] <= row["p95"] <= row["p99"]


class TestAdvancedAggregations:
    """Tests for advanced aggregation functions."""

    def test_values_aggregation(self, sample_user_info):
        """
        Test values() aggregation - collect unique values.
        
        stats values(role) as roles by department
        """
        cmd = 'cache=user_info | stats values(role) as roles by department'
        result = CommandExecutor(cmd).execute()
        
        assert "department" in result.columns
        assert "roles" in result.columns
        
        # Each roles entry should be a list
        for _, row in result.iterrows():
            dept = row["department"]
            expected_roles = sample_user_info[sample_user_info["department"] == dept]["role"].unique()
            assert set(row["roles"]) == set(expected_roles)

    def test_dc_distinct_count(self, sample_web_logs):
        """
        Test dc() - distinct count.
        
        stats dc(ip) as unique_ips by host
        """
        cmd = 'cache=web_logs | stats dc(ip) as unique_ips by host'
        result = CommandExecutor(cmd).execute()
        
        assert "host" in result.columns
        assert "unique_ips" in result.columns
        
        for host in sample_web_logs["host"].unique():
            host_data = sample_web_logs[sample_web_logs["host"] == host]
            result_row = result[result["host"] == host]
            expected_dc = host_data["ip"].nunique()
            assert result_row["unique_ips"].iloc[0] == expected_dc

    def test_first_last_aggregation(self, sample_orders):
        """
        Test first() and last() aggregations.
        
        stats first(order_date) as first_order, last(order_date) as last_order by customer_id
        """
        cmd = 'cache=orders | stats first(order_date) as first_order, last(order_date) as last_order by customer_id'
        result = CommandExecutor(cmd).execute()
        
        assert "customer_id" in result.columns
        assert "first_order" in result.columns
        assert "last_order" in result.columns
        
        # Each customer should have valid dates
        assert len(result) > 0


class TestCompleteStatsCommand:
    """Integration tests for complete stats command scenarios."""

    def test_full_stats_analysis(self, sample_web_logs):
        """
        Full statistics analysis combining multiple functions.
        
        stats count as total_requests, sum(bytes) as total_bytes, avg(response_time) as avg_response,
        max(response_time) as max_response, min(response_time) as min_response, 
        stdev(response_time) as std_response by host, status_code
        """
        cmd = '''cache=web_logs | stats count as total_requests, sum(bytes) as total_bytes, avg(response_time) as avg_response, max(response_time) as max_response, min(response_time) as min_response, stdev(response_time) as std_response by host, status_code'''
        result = CommandExecutor(cmd).execute()
        
        expected_cols = ["host", "status_code", "total_requests", "total_bytes",
                        "avg_response", "max_response", "min_response", "std_response"]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"
        
        # Data integrity checks
        assert result["total_requests"].sum() == len(sample_web_logs)
        assert result["total_bytes"].sum() == sample_web_logs["bytes"].sum()

    def test_endpoint_performance_analysis(self, sample_web_logs):
        """
        Endpoint performance analysis with percentiles.
        
        stats perc50(response_time) as p50, perc75(response_time) as p75,
        perc90(response_time) as p90, perc95(response_time) as p95,
        perc99(response_time) as p99, avg(response_time) as avg_time by uri, method
        """
        cmd = '''cache=web_logs | stats perc50(response_time) as p50, perc75(response_time) as p75, perc90(response_time) as p90, perc95(response_time) as p95, perc99(response_time) as p99, avg(response_time) as avg_time by uri, method'''
        result = CommandExecutor(cmd).execute()
        
        # Verify structure
        assert "uri" in result.columns
        assert "method" in result.columns
        
        # Number of groups should match
        expected_groups = sample_web_logs.groupby(["uri", "method"]).ngroups
        assert len(result) == expected_groups

