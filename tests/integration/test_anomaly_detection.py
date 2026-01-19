"""
Integration tests - Anomaly detection workflows.

Complex pipelines for detecting anomalies in metrics
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestCPUAnomalyDetection:
    """Tests for CPU anomaly detection workflow."""

    def test_cpu_threshold_anomaly(self, sample_server_metrics):
        """Detect CPU anomalies using threshold."""
        cmd = '''cache=server_metrics
        | stats avg(cpu_usage) as baseline, stdev(cpu_usage) as std by host
        | eval threshold=baseline+(2*std)'''
        result = CommandExecutor(cmd).execute()

        assert "baseline" in result.columns
        assert "std" in result.columns
        assert "threshold" in result.columns
        # Threshold should be higher than baseline
        assert all(result["threshold"] > result["baseline"])

    def test_cpu_anomaly_with_if(self, sample_server_metrics):
        """Detect CPU anomalies using if condition."""
        cmd = '''cache=server_metrics
        | eval high_cpu=if(cpu_usage > 80, 1, 0)
        | stats sum(high_cpu) as anomaly_count by host'''
        result = CommandExecutor(cmd).execute()

        assert "anomaly_count" in result.columns
        assert "host" in result.columns


class TestMemoryAnomalyDetection:
    """Tests for memory anomaly detection workflow."""

    def test_memory_threshold_anomaly(self, sample_server_metrics):
        """Detect memory anomalies using threshold."""
        cmd = '''cache=server_metrics
        | stats avg(memory_usage) as avg_mem, stdev(memory_usage) as std_mem by host
        | eval upper_limit=avg_mem+(2*std_mem)'''
        result = CommandExecutor(cmd).execute()

        assert "avg_mem" in result.columns
        assert "upper_limit" in result.columns


class TestCombinedResourceAnomaly:
    """Tests for combined resource anomaly detection."""

    def test_combined_cpu_and_memory(self, sample_server_metrics):
        """Detect combined high CPU and memory."""
        cmd = '''cache=server_metrics
        | eval high_cpu=if(cpu_usage>80, 1, 0)
        | eval high_mem=if(memory_usage>80, 1, 0)
        | eval critical=if((high_cpu = 1) AND (high_mem = 1), 1, 0)
        | stats sum(critical) as critical_count by host'''
        result = CommandExecutor(cmd).execute()

        assert "critical_count" in result.columns


class TestStatisticalAnomalyDetection:
    """Tests for statistical anomaly detection."""

    def test_zscore_anomaly(self, sample_server_metrics):
        """Detect anomalies using z-score approach."""
        # Calculate overall stats
        stats_cmd = 'cache=server_metrics | stats avg(cpu_usage) as global_avg, stdev(cpu_usage) as global_std'
        stats_result = CommandExecutor(stats_cmd).execute()
        
        global_avg = stats_result["global_avg"].iloc[0]
        global_std = stats_result["global_std"].iloc[0]

        # Apply z-score threshold using simple eval
        cmd = f'cache=server_metrics | eval threshold={global_avg + 2 * global_std} | eval is_anomaly=if(cpu_usage > threshold, 1, 0)'
        result = CommandExecutor(cmd).execute()

        assert "threshold" in result.columns
        assert "is_anomaly" in result.columns


class TestPercentileBasedAnomaly:
    """Tests for percentile-based anomaly detection."""

    def test_p95_anomaly(self, sample_server_metrics):
        """Detect anomalies above 95th percentile."""
        # Get p95 threshold
        p95_cmd = 'cache=server_metrics | stats perc95(response_time) as p95'
        p95_result = CommandExecutor(p95_cmd).execute()
        p95 = p95_result["p95"].iloc[0]

        # Find anomalies
        cmd = f'''cache=server_metrics
        | where response_time > {p95}'''
        result = CommandExecutor(cmd).execute()

        # All results should be above p95
        if len(result) > 0:
            assert all(result["response_time"] > p95)

