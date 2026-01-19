"""
Integration tests - Log analysis workflows.

Complex pipelines for log analysis
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestErrorLogAnalysis:
    """Tests for error log analysis workflows."""

    def test_error_summary(self, sample_web_logs):
        """Summarize errors by status code."""
        cmd = '''cache=web_logs
        | where status_code >= 400
        | stats count as error_count by status_code
        '''
        result = CommandExecutor(cmd).execute()

        if len(result) > 0:
            assert "status_code" in result.columns
            assert "error_count" in result.columns

    def test_error_rate_by_endpoint(self, sample_web_logs):
        """Calculate error rate by endpoint."""
        cmd = '''cache=web_logs
        | eval is_error=if(status_code >= 400, 1, 0)
        | stats sum(is_error) as errors, count as total by uri
        | eval error_rate=if(total>0, (errors/total)*100, 0)
        | where error_rate > 0'''
        result = CommandExecutor(cmd).execute()

        assert "error_rate" in result.columns


class TestAccessPatternAnalysis:
    """Tests for access pattern analysis."""

    def test_top_endpoints(self, sample_web_logs):
        """Find top accessed endpoints."""
        cmd = '''cache=web_logs
        | stats count as hits by uri
        '''
        result = CommandExecutor(cmd).execute()

        assert "uri" in result.columns
        assert "hits" in result.columns

    def test_unique_users_by_endpoint(self, sample_web_logs):
        """Count unique users by endpoint."""
        cmd = '''cache=web_logs
        | stats dc(ip) as unique_visitors by uri'''
        result = CommandExecutor(cmd).execute()

        assert "unique_visitors" in result.columns


class TestLogExtractionPipeline:
    """Tests for log extraction and analysis."""

    def test_extract_and_aggregate(self, sample_app_logs):
        """Extract log level and aggregate."""
        cmd = '''cache=app_logs
        | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)"
        | stats count as log_count by level'''
        result = CommandExecutor(cmd).execute()

        assert "level" in result.columns
        assert "log_count" in result.columns


class TestCombinedLogAnalysis:
    """Tests for combining multiple log sources."""

    def test_multi_source_analysis(self):
        """Analyze logs from multiple sources."""
        # Create sample log data
        web_logs = pd.DataFrame({
            "source": ["web"] * 3,
            "status": [200, 404, 500],
            "count": [100, 10, 5],
        })
        app_logs = pd.DataFrame({
            "source": ["app"] * 3,
            "status": ["INFO", "WARN", "ERROR"],
            "count": [200, 20, 10],
        })
        register_cache("web_summary", web_logs)
        register_cache("app_summary", app_logs)

        cmd = '''cache=web_summary | append [search index="app_summary"]'''
        result = CommandExecutor(cmd).execute()

        assert len(result) == 6
        assert set(result["source"]) == {"web", "app"}

