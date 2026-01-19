"""
Tests for anomaly detection - Threshold analysis and multi-index queries.

Covers:
- Baseline calculation with standard deviation
- Threshold-based anomaly detection
- Multi-index union queries
- Complex anomaly detection pipelines
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestBaselineCalculation:
    """Tests for calculating baselines for anomaly detection."""

    def test_calculate_baseline_stats(self, sample_server_metrics):
        """
        Calculate baseline statistics for anomaly detection.
        
        stats avg(cpu_usage) as baseline_cpu, stdev(cpu_usage) as std_cpu by host
        """
        cmd = 'cache=server_metrics | stats avg(cpu_usage) as baseline_cpu, stdev(cpu_usage) as std_cpu by host'
        result = CommandExecutor(cmd).execute()
        
        assert "host" in result.columns
        assert "baseline_cpu" in result.columns
        assert "std_cpu" in result.columns
        
        # Should have one row per host
        assert len(result) == 3

    def test_calculate_threshold(self, sample_server_metrics):
        """
        Calculate threshold based on baseline + 2*stdev.
        
        stats avg(cpu_usage) as baseline_cpu, stdev(cpu_usage) as std_cpu by host | 
        eval threshold=baseline_cpu + (2 * std_cpu)
        """
        cmd = 'cache=server_metrics | stats avg(cpu_usage) as baseline_cpu, stdev(cpu_usage) as std_cpu by host | eval threshold=baseline_cpu + (2 * std_cpu)'
        result = CommandExecutor(cmd).execute()
        
        assert "threshold" in result.columns
        
        # Threshold should be greater than baseline
        assert all(result["threshold"] > result["baseline_cpu"])

    def test_baseline_with_percentiles(self, sample_server_metrics):
        """
        Calculate baseline using percentiles for more robust thresholds.
        
        stats perc50(cpu_usage) as median_cpu, perc95(cpu_usage) as p95_cpu, 
        perc99(cpu_usage) as p99_cpu by host
        """
        cmd = 'cache=server_metrics | stats perc50(cpu_usage) as median_cpu, perc95(cpu_usage) as p95_cpu, perc99(cpu_usage) as p99_cpu by host'
        result = CommandExecutor(cmd).execute()
        
        assert "median_cpu" in result.columns
        assert "p95_cpu" in result.columns
        assert "p99_cpu" in result.columns
        
        # Percentiles should be in order
        for _, row in result.iterrows():
            assert row["median_cpu"] <= row["p95_cpu"] <= row["p99_cpu"]


class TestAnomalyDetection:
    """Tests for anomaly detection logic."""

    def test_simple_threshold_anomaly(self):
        """
        Test simple threshold-based anomaly detection.
        
        eval is_anomaly=if(cpu_usage > threshold, 1, 0)
        """
        df = pd.DataFrame({
            "host": ["server01", "server01", "server01", "server02", "server02"],
            "cpu_usage": [30, 85, 95, 40, 50],
        })
        register_cache("cpu_data", df)
        
        cmd = 'cache=cpu_data | eval is_anomaly=if(cpu_usage > 80, 1, 0)'
        result = CommandExecutor(cmd).execute()
        
        assert "is_anomaly" in result.columns
        assert result["is_anomaly"].tolist() == [0, 1, 1, 0, 0]

    def test_anomaly_with_filter(self):
        """
        Test anomaly detection followed by filtering anomalies.
        
        eval is_anomaly=if(...) | where is_anomaly=1
        """
        df = pd.DataFrame({
            "host": ["server01", "server01", "server01", "server02", "server02"],
            "cpu_usage": [30, 85, 95, 40, 50],
        })
        register_cache("cpu_data", df)
        
        cmd = 'cache=cpu_data | eval is_anomaly=if(cpu_usage > 80, 1, 0) | where is_anomaly=1'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 2
        assert all(result["cpu_usage"] > 80)

    def test_complete_anomaly_detection_pipeline(self, sample_server_metrics):
        """
        Complete anomaly detection pipeline from user example.
        
        stats avg(cpu_usage) as baseline_cpu, stdev(cpu_usage) as std_cpu by host | 
        eval threshold=baseline_cpu + (2 * std_cpu) | 
        join host [search latest=-5m | stats max(cpu_usage) as current_cpu by host] | 
        eval is_anomaly=if(current_cpu > threshold, 1, 0) | where is_anomaly=1
        """
        # First calculate baseline from historical data
        cmd_baseline = 'cache=server_metrics | stats avg(cpu_usage) as baseline_cpu, stdev(cpu_usage) as std_cpu by host | eval threshold=baseline_cpu + (2 * std_cpu)'
        baseline = CommandExecutor(cmd_baseline).execute()
        register_cache("baseline", baseline)
        
        # Get current metrics (simulate)
        current = pd.DataFrame({
            "host": ["server01", "server02", "server03"],
            "current_cpu": [35, 90, 40],  # server02 is high
        })
        register_cache("current_metrics", current)
        
        # Join and detect
        cmd = 'cache=baseline | join host [search index="current_metrics" | stats first(current_cpu) as current_cpu by host] | eval is_anomaly=if(current_cpu > threshold, 1, 0)'
        result = CommandExecutor(cmd).execute()
        
        assert "is_anomaly" in result.columns
        assert "threshold" in result.columns
        assert "current_cpu" in result.columns


class TestMultiIndexQueries:
    """Tests for multi-index union queries."""

    def test_union_two_indexes(self, sample_app_logs, sample_error_logs):
        """
        Test union query from two indexes.
        
        (index="app_logs" OR index="error_logs") sourcetype=application
        """
        # This requires union/append functionality
        cmd = 'cache=app_logs | append [search index="error_logs"]'
        result = CommandExecutor(cmd).execute()
        
        # Should contain rows from both sources
        assert len(result) == len(sample_app_logs) + len(sample_error_logs)

    def test_union_with_coalesce(self, sample_app_logs, sample_error_logs):
        """
        Test union with coalesce for normalizing fields.
        
        (index="app_logs" OR index="error_logs") | eval log_level=coalesce(level, severity)
        """
        # First add a level column to app_logs via rex
        cmd = '''cache=app_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)" | append [search index="error_logs"] | eval log_level=coalesce(level, severity)'''
        result = CommandExecutor(cmd).execute()
        
        assert "log_level" in result.columns

    def test_multi_index_with_stats(self, sample_app_logs, sample_error_logs):
        """
        Test multi-index query with stats aggregation.
        
        (index="app_logs" OR index="error_logs") | eval log_level=coalesce(level, severity) | 
        stats count, values(message) as messages by host, log_level
        """
        # Setup: add host column to app_logs if needed
        app_logs_with_host = sample_app_logs.copy()
        app_logs_with_host["host"] = "app01"
        app_logs_with_host["level"] = ["INFO", "WARN", "ERROR", "INFO", "DEBUG",
                                       "ERROR", "INFO", "WARN", "INFO", "ERROR"]
        register_cache("app_logs_ext", app_logs_with_host)
        
        cmd = '''cache=app_logs_ext | append [search index="error_logs"] | eval log_level=coalesce(level, severity) | stats count as n by host, log_level'''
        result = CommandExecutor(cmd).execute()
        
        assert "host" in result.columns
        assert "log_level" in result.columns
        assert "n" in result.columns


class TestAdvancedAnomalyScenarios:
    """Tests for advanced anomaly detection scenarios."""

    def test_z_score_anomaly_detection(self):
        """
        Test Z-score based anomaly detection.
        
        eval z_score=(value - mean) / stdev | where abs(z_score) > 2
        """
        np.random.seed(42)
        # Create data with some anomalies
        values = np.random.normal(50, 10, 100).tolist()
        values.extend([100, 5, 95, 10])  # Add anomalies
        
        df = pd.DataFrame({
            "id": range(len(values)),
            "value": values,
        })
        register_cache("zscore_data", df)
        
        # Calculate mean and stdev, then z-score
        cmd = '''cache=zscore_data | stats avg(value) as mean_val, stdev(value) as std_val'''
        stats_result = CommandExecutor(cmd).execute()
        mean_val = stats_result["mean_val"].iloc[0]
        std_val = stats_result["std_val"].iloc[0]
        
        # Add as constants and calculate z-score
        df["mean"] = mean_val
        df["stdev"] = std_val
        register_cache("zscore_data", df)
        
        cmd = 'cache=zscore_data | eval z_score=(value - mean) / stdev | where abs(z_score) > 2'
        result = CommandExecutor(cmd).execute()
        
        # Should detect the anomalies
        assert len(result) > 0

    def test_trend_based_anomaly(self, sample_server_metrics):
        """
        Test detecting anomalies based on trend changes.
        
        bucket _time span=15m | stats avg(cpu_usage) as avg_cpu by host, _time |
        eval is_spike=if(avg_cpu > 70, 1, 0)
        """
        cmd = '''cache=server_metrics | bucket _time span=15m | stats avg(cpu_usage) as avg_cpu by host, _time | eval is_spike=if(avg_cpu > 70, 1, 0)'''
        result = CommandExecutor(cmd).execute()
        
        assert "is_spike" in result.columns
        # server02 in our fixture has high CPU after a certain point
        assert 1 in result["is_spike"].values

    def test_comparative_anomaly(self, sample_server_metrics):
        """
        Test anomaly detection by comparing to peer group.
        
        stats avg(cpu_usage) as avg_cpu | eval global_avg=avg_cpu |
        join type=cross [...] | eval deviation=host_cpu - global_avg
        """
        # First get global average
        cmd_global = 'cache=server_metrics | stats avg(cpu_usage) as global_avg'
        global_result = CommandExecutor(cmd_global).execute()
        global_avg = global_result["global_avg"].iloc[0]
        
        # Get per-host average
        cmd_host = 'cache=server_metrics | stats avg(cpu_usage) as host_avg by host'
        host_result = CommandExecutor(cmd_host).execute()
        host_result["global_avg"] = global_avg
        register_cache("host_comparison", host_result)
        
        # Calculate deviation
        cmd = 'cache=host_comparison | eval deviation=host_avg - global_avg | eval is_high=if(deviation > 10, 1, 0)'
        result = CommandExecutor(cmd).execute()
        
        assert "deviation" in result.columns
        assert "is_high" in result.columns


class TestMemoryAndResourceAnomalies:
    """Tests for memory and resource-related anomaly detection."""

    def test_memory_usage_anomaly(self, sample_server_metrics):
        """Test detecting memory usage anomalies."""
        cmd = 'cache=server_metrics | stats avg(memory_usage) as avg_mem, max(memory_usage) as max_mem, stdev(memory_usage) as std_mem by host'
        result = CommandExecutor(cmd).execute()
        
        assert "avg_mem" in result.columns
        assert "max_mem" in result.columns
        assert "std_mem" in result.columns

    def test_combined_resource_anomaly(self, sample_server_metrics):
        """Test detecting combined CPU + memory anomalies."""
        cmd = '''cache=server_metrics | eval high_cpu=if(cpu_usage > 70, 1, 0) | eval high_mem=if(memory_usage > 80, 1, 0) | eval is_critical=if(high_cpu=1 AND high_mem=1, 1, 0) | stats sum(is_critical) as critical_count by host'''
        result = CommandExecutor(cmd).execute()
        
        assert "critical_count" in result.columns


class TestAnomalyAlertingPipelines:
    """Tests for anomaly alerting pipelines."""

    def test_anomaly_count_by_host(self, sample_server_metrics):
        """
        Count anomalies per host for alerting.
        
        eval is_anomaly=if(cpu_usage > 70, 1, 0) | stats sum(is_anomaly) as anomaly_count by host | 
        where anomaly_count > 10
        """
        cmd = '''cache=server_metrics | eval is_anomaly=if(cpu_usage > 70, 1, 0) | stats sum(is_anomaly) as anomaly_count by host | where anomaly_count > 10'''
        result = CommandExecutor(cmd).execute()
        
        if len(result) > 0:
            assert all(result["anomaly_count"] > 10)

    def test_anomaly_rate_calculation(self, sample_server_metrics):
        """
        Calculate anomaly rate for alerting.
        
        eval is_anomaly=if(cpu_usage > 70, 1, 0) | stats sum(is_anomaly) as anomalies, count as total by host |
        eval anomaly_rate=(anomalies/total)*100
        """
        cmd = '''cache=server_metrics | eval is_anomaly=if(cpu_usage > 70, 1, 0) | stats sum(is_anomaly) as anomalies, count as total by host | eval anomaly_rate=(anomalies/total)*100'''
        result = CommandExecutor(cmd).execute()
        
        assert "anomaly_rate" in result.columns
        # Rate should be between 0 and 100
        assert all((result["anomaly_rate"] >= 0) & (result["anomaly_rate"] <= 100))

    def test_full_anomaly_detection_report(self, sample_server_metrics):
        """
        Complete anomaly detection report generation.
        """
        cmd = '''cache=server_metrics | stats avg(cpu_usage) as avg_cpu, max(cpu_usage) as max_cpu, stdev(cpu_usage) as std_cpu, avg(memory_usage) as avg_mem, max(memory_usage) as max_mem by host | eval cpu_threshold=avg_cpu + (2 * std_cpu) | eval status=case(max_cpu > cpu_threshold, "CRITICAL", max_cpu > avg_cpu + std_cpu, "WARNING", 1=1, "NORMAL")'''
        result = CommandExecutor(cmd).execute()
        
        assert "status" in result.columns
        valid_statuses = {"CRITICAL", "WARNING", "NORMAL"}
        assert set(result["status"].unique()).issubset(valid_statuses)

