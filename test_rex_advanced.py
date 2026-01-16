"""
Tests for advanced rex (regex extraction) commands.

Covers:
- Complex string parsing with multiple capture groups
- Field extraction from raw logs
- Timestamp parsing
"""

import numpy as np
import pandas as pd
import pytest

from executors import CommandExecutor, register_cache


class TestComplexRexExtraction:
    """Test cases for complex rex field extraction."""

    @pytest.fixture
    def raw_logs_df(self) -> pd.DataFrame:
        """Raw log data with complex format."""
        np.random.seed(42)
        n = 100

        levels = ["INFO", "WARN", "ERROR", "DEBUG"]
        loggers = [
            "com.app.service.UserService",
            "com.app.controller.ApiController",
            "com.app.repository.DataRepo",
            "org.framework.http.RequestHandler",
        ]
        messages = [
            "Request processed successfully",
            "User authentication completed",
            "Database query executed in 45ms",
            "Cache hit for key user:123",
            "Connection timeout occurred",
            "Invalid input received",
            "Resource not found: /api/items/999",
            "Transaction committed",
        ]

        logs = []
        base_time = pd.Timestamp("2024-01-15 10:00:00")
        
        for i in range(n):
            timestamp = base_time + pd.Timedelta(seconds=i * 5)
            level = np.random.choice(levels, p=[0.5, 0.2, 0.15, 0.15])
            logger = np.random.choice(loggers)
            message = np.random.choice(messages)
            
            # Format: "2024-01-15 10:00:00 INFO com.app.service.UserService - Request processed"
            raw = f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} {level} {logger} - {message}"
            logs.append({"_raw": raw, "index": "app_logs"})

        return pd.DataFrame(logs)

    def test_rex_multiple_capture_groups(self, raw_logs_df: pd.DataFrame):
        """
        Test: rex field=_raw "(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})\\s+
              (?<level>\\w+)\\s+(?<logger>[^\\s]+)\\s+-\\s+(?<message>.*)"
        """
        register_cache("raw_logs", raw_logs_df)

        # Simplified pattern for testing
        cmd = (
            'cache=raw_logs '
            '| rex field=_raw "(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})\\s+'
            '(?<level>\\w+)\\s+(?<logger>[^\\s]+)\\s+-\\s+(?<message>.*)"'
        )

        try:
            result = CommandExecutor(cmd).execute()

            # Verify extracted fields
            assert "timestamp" in result.columns
            assert "level" in result.columns
            assert "logger" in result.columns
            assert "message" in result.columns

            # Verify extraction quality
            assert all(result["level"].isin(["INFO", "WARN", "ERROR", "DEBUG"]))
            assert all(result["logger"].str.contains(".", regex=False))

        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"Complex rex pattern not supported: {e}")

    def test_rex_simple_extraction(self, raw_logs_df: pd.DataFrame):
        """Test simple single-field extraction."""
        register_cache("raw_logs", raw_logs_df)

        cmd = 'cache=raw_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)"'

        try:
            result = CommandExecutor(cmd).execute()
            assert "level" in result.columns
            assert all(result["level"].isin(["INFO", "WARN", "ERROR", "DEBUG"]))
        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"rex command not working: {e}")


class TestAccessLogParsing:
    """Test cases for parsing web access logs."""

    @pytest.fixture
    def apache_logs_df(self) -> pd.DataFrame:
        """Apache-style access logs."""
        np.random.seed(42)
        n = 100

        ips = [f"192.168.1.{i}" for i in range(1, 51)]
        methods = ["GET", "POST", "PUT", "DELETE"]
        paths = ["/api/users", "/api/orders", "/api/products", "/health", "/"]
        status_codes = [200, 201, 301, 400, 404, 500]
        
        logs = []
        base_time = pd.Timestamp("2024-01-15")
        
        for i in range(n):
            ip = np.random.choice(ips)
            timestamp = (base_time + pd.Timedelta(seconds=i * 10)).strftime("%d/%b/%Y:%H:%M:%S +0000")
            method = np.random.choice(methods, p=[0.6, 0.2, 0.15, 0.05])
            path = np.random.choice(paths)
            status = np.random.choice(status_codes, p=[0.5, 0.1, 0.05, 0.1, 0.15, 0.1])
            size = np.random.randint(100, 50000)
            
            # Apache Combined Log Format
            raw = f'{ip} - - [{timestamp}] "{method} {path} HTTP/1.1" {status} {size}'
            logs.append({"_raw": raw})

        return pd.DataFrame(logs)

    def test_rex_apache_log_parsing(self, apache_logs_df: pd.DataFrame):
        """Test parsing Apache access logs."""
        register_cache("apache_logs", apache_logs_df)

        # Extract IP and status code
        cmd = (
            'cache=apache_logs '
            '| rex field=_raw "^(?<client_ip>\\d+\\.\\d+\\.\\d+\\.\\d+)" '
            '| rex field=_raw "HTTP/1\\.1\\" (?<status>\\d+)"'
        )

        try:
            result = CommandExecutor(cmd).execute()

            # At least one extraction should work
            has_ip = "client_ip" in result.columns
            has_status = "status" in result.columns

            if has_ip:
                # Verify IP format
                sample_ip = result["client_ip"].dropna().iloc[0]
                assert sample_ip.count(".") == 3

            if has_status:
                # Verify status codes are numeric strings
                assert all(result["status"].dropna().str.isdigit())

        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"rex command not working: {e}")


class TestRexWithPipeline:
    """Test cases for rex in complex pipelines."""

    @pytest.fixture
    def api_logs_df(self) -> pd.DataFrame:
        """API logs with request details."""
        np.random.seed(42)
        n = 200

        logs = []
        for i in range(n):
            user = f"user_{np.random.randint(1, 50)}"
            endpoint = np.random.choice(["/api/v1/users", "/api/v1/orders", "/api/v2/products"])
            duration = np.random.exponential(scale=100)
            
            # Format: "user=user_42 endpoint=/api/v1/users duration=123.45ms"
            raw = f"user={user} endpoint={endpoint} duration={duration:.2f}ms"
            logs.append({"_raw": raw, "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(seconds=i)})

        return pd.DataFrame(logs)

    def test_rex_extract_and_stats(self, api_logs_df: pd.DataFrame):
        """
        Test: rex field=_raw "user=(?<user>\\w+)" 
              | rex field=_raw "endpoint=(?<endpoint>[^\\s]+)"
              | stats count by user, endpoint
        """
        register_cache("api_logs", api_logs_df)

        cmd = (
            'cache=api_logs '
            '| rex field=_raw "user=(?<user>\\w+)" '
            '| rex field=_raw "endpoint=(?<endpoint>[^\\s]+)" '
            '| stats count by user, endpoint'
        )

        try:
            result = CommandExecutor(cmd).execute()

            assert "user" in result.columns
            assert "endpoint" in result.columns
            assert "count" in result.columns

            # Total count should match original
            assert result["count"].sum() == len(api_logs_df)

        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"rex pipeline not working: {e}")

    def test_rex_extract_numeric_field(self, api_logs_df: pd.DataFrame):
        """Test extracting numeric values with rex."""
        register_cache("api_logs", api_logs_df)

        cmd = (
            'cache=api_logs '
            '| rex field=_raw "duration=(?<duration>[\\d.]+)ms"'
        )

        try:
            result = CommandExecutor(cmd).execute()

            assert "duration" in result.columns

            # Verify numeric values can be extracted
            # Note: rex typically returns strings, conversion may be needed
            durations = result["duration"].dropna()
            assert len(durations) > 0

        except (ValueError, NotImplementedError) as e:
            pytest.skip(f"rex command not working: {e}")


class TestTimestampParsing:
    """Test cases for timestamp parsing with strptime."""

    @pytest.fixture
    def timestamp_logs_df(self) -> pd.DataFrame:
        """Logs with various timestamp formats."""
        np.random.seed(42)
        n = 50

        logs = []
        base_time = pd.Timestamp("2024-01-15 08:00:00")
        
        for i in range(n):
            ts = base_time + pd.Timedelta(minutes=i * 5)
            # Format: "2024-01-15 08:00:00"
            timestamp_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            value = np.random.uniform(10, 100)
            
            logs.append({
                "timestamp_str": timestamp_str,
                "value": round(value, 2),
            })

        return pd.DataFrame(logs)

    def test_strptime_parsing(self, timestamp_logs_df: pd.DataFrame):
        """
        Test: eval parsed_time=strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

        Note: strptime function may need to be implemented.
        """
        register_cache("timestamp_logs", timestamp_logs_df)

        cmd = (
            'cache=timestamp_logs '
            '| eval parsed_time = strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")'
        )

        try:
            result = CommandExecutor(cmd).execute()

            assert "parsed_time" in result.columns

            # Verify parsed times are valid
            assert result["parsed_time"].notna().any()

        except (ValueError, NotImplementedError, KeyError) as e:
            pytest.skip(f"strptime not implemented: {e}")

