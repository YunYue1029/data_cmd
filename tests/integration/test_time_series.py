"""
Integration tests - Time series analysis workflows.

Complex pipelines for time series analysis
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestHourlyAggregation:
    """Tests for hourly time series aggregation."""

    def test_hourly_request_rate(self, sample_web_logs):
        """Calculate hourly request rate."""
        cmd = '''cache=web_logs
        | bucket _time span=1h
        | stats count as requests by _time, host'''
        result = CommandExecutor(cmd).execute()

        assert "requests" in result.columns
        assert "_time" in result.columns
        assert "host" in result.columns

    def test_hourly_error_rate(self, sample_web_logs):
        """Calculate hourly error rate."""
        cmd = '''cache=web_logs
        | eval is_error=if(status_code >= 400, 1, 0)
        | bucket _time span=1h
        | stats sum(is_error) as errors, count as total by _time
        | eval error_rate=if(total>0, (errors/total)*100, 0)'''
        result = CommandExecutor(cmd).execute()

        assert "error_rate" in result.columns


class TestRollingMetrics:
    """Tests for rolling/sliding window metrics."""

    def test_bucket_for_rolling_avg(self, sample_server_metrics):
        """Use bucket for rolling-like aggregation."""
        cmd = '''cache=server_metrics
        | bucket _time span=5m
        | stats avg(cpu_usage) as avg_cpu, 
                min(cpu_usage) as min_cpu, 
                max(cpu_usage) as max_cpu by _time'''
        result = CommandExecutor(cmd).execute()

        assert "avg_cpu" in result.columns
        assert "min_cpu" in result.columns
        assert "max_cpu" in result.columns


class TestTrendAnalysis:
    """Tests for trend analysis workflows."""

    def test_hourly_trend(self, sample_server_metrics):
        """Analyze hourly trend."""
        cmd = '''cache=server_metrics
        | eval hour=strftime(_time, "%H")
        | stats avg(cpu_usage) as avg_cpu by hour
        '''
        result = CommandExecutor(cmd).execute()

        assert "hour" in result.columns
        assert "avg_cpu" in result.columns


class TestSessionAnalysis:
    """Tests for session-based analysis."""

    def test_session_duration(self, sample_session_logs):
        """Calculate session duration."""
        cmd = '''cache=session_logs
        | transaction session_id
        | stats avg(duration) as avg_duration, 
                perc50(duration) as median_duration,
                perc95(duration) as p95_duration'''
        result = CommandExecutor(cmd).execute()

        assert "avg_duration" in result.columns
        assert "median_duration" in result.columns
        assert "p95_duration" in result.columns

    def test_session_event_count(self, sample_session_logs):
        """Analyze events per session."""
        cmd = '''cache=session_logs
        | transaction session_id
        | stats avg(event_count) as avg_events, 
                max(event_count) as max_events'''
        result = CommandExecutor(cmd).execute()

        assert "avg_events" in result.columns
        assert "max_events" in result.columns

