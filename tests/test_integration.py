"""
Integration tests - Complex pipelines combining multiple commands.

Covers:
- Anomaly detection pipelines
- Complete data analysis workflows
- Multi-step data transformations
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestAnomalyDetectionPipelines:
    """Integration tests for anomaly detection workflows."""

    def test_baseline_and_threshold_pipeline(self, sample_server_metrics):
        """
        Complete baseline calculation and threshold pipeline.
        
        stats avg(cpu_usage) as baseline, stdev(cpu_usage) as std by host |
        eval threshold = baseline + (2 * std)
        """
        cmd = '''cache=server_metrics | stats avg(cpu_usage) as baseline_cpu, stdev(cpu_usage) as std_cpu by host | eval threshold=baseline_cpu + (2 * std_cpu)'''
        result = CommandExecutor(cmd).execute()

        assert "baseline_cpu" in result.columns
        assert "std_cpu" in result.columns
        assert "threshold" in result.columns
        assert all(result["threshold"] > result["baseline_cpu"])

    def test_z_score_anomaly_pipeline(self):
        """
        Z-score based anomaly detection pipeline.
        
        Calculate mean/stdev, compute z-score, filter outliers.
        """
        np.random.seed(42)
        values = np.random.normal(50, 10, 100).tolist()
        values.extend([100, 5, 95, 10])  # Add anomalies

        df = pd.DataFrame({
            "id": range(len(values)),
            "value": values,
        })
        register_cache("zscore_data", df)

        # Step 1: Calculate mean and stdev
        cmd_stats = 'cache=zscore_data | stats avg(value) as mean_val, stdev(value) as std_val'
        stats_result = CommandExecutor(cmd_stats).execute()
        mean_val = stats_result["mean_val"].iloc[0]
        std_val = stats_result["std_val"].iloc[0]

        # Step 2: Add constants and calculate z-score
        df["mean"] = mean_val
        df["stdev"] = std_val
        register_cache("zscore_data", df)

        cmd = 'cache=zscore_data | eval z_score=(value - mean) / stdev | where abs(z_score) > 2'
        result = CommandExecutor(cmd).execute()

        assert len(result) > 0  # Should detect outliers

    def test_combined_resource_anomaly_pipeline(self, sample_server_metrics):
        """
        Detect combined CPU + memory anomalies.
        
        eval high_cpu=if(...) | eval high_mem=if(...) | 
        eval is_critical=if(high_cpu AND high_mem) | stats by host
        """
        cmd = '''cache=server_metrics | eval high_cpu=if(cpu_usage > 70, 1, 0) | eval high_mem=if(memory_usage > 80, 1, 0) | eval is_critical=if(high_cpu=1 AND high_mem=1, 1, 0) | stats sum(is_critical) as critical_count by host'''
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "critical_count" in result.columns

    def test_anomaly_rate_calculation_pipeline(self, sample_server_metrics):
        """
        Calculate anomaly rate per host.
        
        eval is_anomaly=if(...) | stats sum, count by host | eval rate=(sum/count)*100
        """
        cmd = '''cache=server_metrics | eval is_anomaly=if(cpu_usage > 70, 1, 0) | stats sum(is_anomaly) as anomalies, count as total by host | eval anomaly_rate=(anomalies/total)*100'''
        result = CommandExecutor(cmd).execute()

        assert "anomaly_rate" in result.columns
        assert all((result["anomaly_rate"] >= 0) & (result["anomaly_rate"] <= 100))

    def test_full_anomaly_detection_report(self, sample_server_metrics):
        """
        Complete anomaly detection report with status classification.
        
        stats + eval threshold + eval status with case
        """
        cmd = '''cache=server_metrics | stats avg(cpu_usage) as avg_cpu, max(cpu_usage) as max_cpu, stdev(cpu_usage) as std_cpu, avg(memory_usage) as avg_mem, max(memory_usage) as max_mem by host | eval cpu_threshold=avg_cpu + (2 * std_cpu) | eval status=case(max_cpu > cpu_threshold, "CRITICAL", max_cpu > avg_cpu + std_cpu, "WARNING", 1=1, "NORMAL")'''
        result = CommandExecutor(cmd).execute()

        assert "status" in result.columns
        valid_statuses = {"CRITICAL", "WARNING", "NORMAL"}
        assert set(result["status"].unique()).issubset(valid_statuses)


class TestTimeSeriesAnalysisPipelines:
    """Integration tests for time series analysis workflows."""

    def test_hourly_aggregation_pipeline(self, sample_server_metrics):
        """
        Hourly time series aggregation.
        
        eval hour=strftime(_time, "%H") | stats avg by host, hour
        """
        cmd = '''cache=server_metrics | eval hour=strftime(_time, "%H") | stats avg(cpu_usage) as avg_cpu, max(memory_usage) as max_mem by host, hour'''
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "hour" in result.columns
        assert "avg_cpu" in result.columns
        assert len(result["hour"].unique()) > 1

    def test_bucket_and_stats_pipeline(self, sample_server_metrics):
        """
        Time bucketing with statistics.
        
        bucket _time span=15m | stats avg by host, _time
        """
        cmd = '''cache=server_metrics | bucket _time span=15m | stats avg(cpu_usage) as avg_cpu by host, _time | eval is_spike=if(avg_cpu > 70, 1, 0)'''
        result = CommandExecutor(cmd).execute()

        assert "is_spike" in result.columns
        assert 1 in result["is_spike"].values

    def test_transaction_session_pipeline(self, sample_user_events):
        """
        Session analysis with transaction grouping.
        
        transaction user_id maxspan=5m | stats avg(duration), count
        """
        cmd = 'cache=user_events | transaction user_id maxspan=5m'
        result = CommandExecutor(cmd).execute()

        assert "duration" in result.columns
        assert "event_count" in result.columns


class TestFinancialAnalysisPipelines:
    """Integration tests for financial analysis workflows."""

    def test_profit_margin_analysis(self, sample_financial_data):
        """
        Complete profit margin calculation and categorization.
        
        eval profit | eval margin | eval category with case | stats by category
        """
        cmd = '''cache=financial | eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0) | eval category=case(profit_margin>20, "High", profit_margin>10, "Medium", profit_margin>0, "Low", 1=1, "Loss") | stats avg(profit_margin) as avg_margin, count as n by category'''
        result = CommandExecutor(cmd).execute()

        assert "category" in result.columns
        assert "avg_margin" in result.columns
        assert "n" in result.columns
        assert result["n"].sum() == len(sample_financial_data)

    def test_order_analysis_with_join(self, sample_orders, sample_products, sample_customers):
        """
        Order analysis with product and customer joins.
        
        join product_id [...] | join customer_id [...] | stats
        """
        cmd = '''cache=orders | join product_id [search index="products"] | stats sum(amount) as total_sales, count as order_count by category'''
        result = CommandExecutor(cmd).execute()

        assert "category" in result.columns
        assert "total_sales" in result.columns
        assert "order_count" in result.columns


class TestLogAnalysisPipelines:
    """Integration tests for log analysis workflows."""

    def test_log_parsing_and_aggregation(self, sample_app_logs):
        """
        Log parsing with rex followed by aggregation.
        
        rex field=_raw "..." | stats count by level
        """
        cmd = '''cache=app_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)" | stats count as n by level'''
        result = CommandExecutor(cmd).execute()

        assert "level" in result.columns
        assert "n" in result.columns

    def test_multi_source_log_analysis(self, sample_app_logs, sample_error_logs):
        """
        Multi-source log analysis with append and coalesce.
        
        append [...] | eval log_level=coalesce(...) | stats by host, level
        """
        app_logs_ext = sample_app_logs.copy()
        app_logs_ext["host"] = "app01"
        app_logs_ext["level"] = ["INFO", "WARN", "ERROR", "INFO", "DEBUG",
                                 "ERROR", "INFO", "WARN", "INFO", "ERROR"]
        register_cache("app_logs_ext", app_logs_ext)

        cmd = '''cache=app_logs_ext | append [search index="error_logs"] | eval log_level=coalesce(level, severity) | stats count as n by host, log_level'''
        result = CommandExecutor(cmd).execute()

        assert "host" in result.columns
        assert "log_level" in result.columns
        expected_total = len(app_logs_ext) + len(sample_error_logs)
        assert result["n"].sum() == expected_total


class TestWebLogAnalysisPipelines:
    """Integration tests for web log analysis workflows."""

    def test_response_time_analysis(self, sample_web_logs):
        """
        Response time analysis with categorization and stats.
        
        eval category=case(...) | stats by category, status_code
        """
        cmd = '''cache=web_logs | eval response_category=case(response_time<100, "fast", response_time<500, "normal", 1=1, "slow") | stats count as n, avg(response_time) as avg_time by response_category, status_code'''
        result = CommandExecutor(cmd).execute()

        assert "response_category" in result.columns
        assert "status_code" in result.columns
        valid_categories = {"fast", "normal", "slow"}
        assert set(result["response_category"].unique()).issubset(valid_categories)

    def test_endpoint_performance_report(self, sample_web_logs):
        """
        Endpoint performance report with percentiles.
        
        stats perc50, perc75, perc90, perc95, perc99, avg by uri, method
        """
        cmd = '''cache=web_logs | stats perc50(response_time) as p50, perc75(response_time) as p75, perc90(response_time) as p90, perc95(response_time) as p95, perc99(response_time) as p99, avg(response_time) as avg_time by uri, method'''
        result = CommandExecutor(cmd).execute()

        assert "uri" in result.columns
        assert "method" in result.columns
        for _, row in result.iterrows():
            assert row["p50"] <= row["p75"] <= row["p90"] <= row["p95"] <= row["p99"]


class TestCompleteAnalyticsWorkflows:
    """End-to-end analytics workflow tests."""

    def test_complete_server_monitoring_workflow(self, sample_server_metrics):
        """
        Complete server monitoring analysis workflow.
        
        1. Bucket by time
        2. Calculate stats per bucket
        3. Identify anomalies
        4. Summarize by host
        """
        # Step 1: Time-bucketed stats
        cmd1 = 'cache=server_metrics | bucket _time span=30m | stats avg(cpu_usage) as avg_cpu, avg(memory_usage) as avg_mem by host, _time'
        bucketed = CommandExecutor(cmd1).execute()
        register_cache("bucketed_metrics", bucketed)

        # Step 2: Identify high usage periods
        cmd2 = 'cache=bucketed_metrics | eval is_high_cpu=if(avg_cpu > 60, 1, 0) | eval is_high_mem=if(avg_mem > 70, 1, 0)'
        flagged = CommandExecutor(cmd2).execute()
        register_cache("flagged_metrics", flagged)

        # Step 3: Summarize by host
        cmd3 = 'cache=flagged_metrics | stats sum(is_high_cpu) as high_cpu_periods, sum(is_high_mem) as high_mem_periods, count as total_periods by host'
        result = CommandExecutor(cmd3).execute()

        assert "host" in result.columns
        assert "high_cpu_periods" in result.columns
        assert "high_mem_periods" in result.columns

    def test_complete_user_behavior_workflow(self, sample_user_events):
        """
        Complete user behavior analysis workflow.
        
        1. Group into sessions
        2. Calculate session metrics
        3. Aggregate by user
        """
        # Step 1: Session grouping
        cmd1 = 'cache=user_events | transaction user_id maxspan=5m'
        sessions = CommandExecutor(cmd1).execute()
        register_cache("sessions", sessions)

        # Step 2: Session stats
        cmd2 = 'cache=sessions | stats avg(duration) as avg_session_duration, avg(event_count) as avg_events_per_session by user_id'
        result = CommandExecutor(cmd2).execute()

        assert "user_id" in result.columns
        assert "avg_session_duration" in result.columns
        assert "avg_events_per_session" in result.columns

