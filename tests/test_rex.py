"""
Tests for rex command - String parsing and field extraction.

Covers:
- Named capture groups for field extraction
- Complex regex patterns
- Multi-field extraction
- Sed mode for replacement
- Log parsing scenarios
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestBasicRexExtraction:
    """Tests for basic regex field extraction."""

    def test_extract_ip_address(self):
        """Test extracting IP address with named group."""
        df = pd.DataFrame({
            "message": [
                "Connection from 192.168.1.100",
                "Request from 10.0.0.50",
                "Error on 172.16.0.1",
            ],
        })
        register_cache("logs", df)
        
        cmd = r'cache=logs | rex field=message "(?<ip>\d+\.\d+\.\d+\.\d+)"'
        result = CommandExecutor(cmd).execute()
        
        assert "ip" in result.columns
        assert result["ip"].tolist() == ["192.168.1.100", "10.0.0.50", "172.16.0.1"]

    def test_extract_uri_endpoint(self, sample_web_logs):
        """
        Test extracting endpoint from URI.
        
        rex field=uri "(?<endpoint>/[^?]+)"
        """
        cmd = r'cache=web_logs | rex field=uri "(?<endpoint>/[^?]+)"'
        result = CommandExecutor(cmd).execute()
        
        assert "endpoint" in result.columns
        # All endpoints should start with /
        assert all(str(e).startswith("/") for e in result["endpoint"].dropna())

    def test_extract_key_value_pairs(self):
        """Test extracting key=value patterns."""
        df = pd.DataFrame({
            "log": [
                "user_id=123 action=login status=success",
                "user_id=456 action=logout status=success",
                "user_id=789 action=purchase status=failed",
            ],
        })
        register_cache("kv_logs", df)
        
        cmd = r'cache=kv_logs | rex field=log "user_id=(?<user_id>\d+)"'
        result = CommandExecutor(cmd).execute()
        
        assert "user_id" in result.columns
        assert result["user_id"].tolist() == ["123", "456", "789"]

    def test_extract_email_address(self):
        """Test extracting email addresses."""
        df = pd.DataFrame({
            "text": [
                "Contact: john@example.com for info",
                "Email jane.doe@company.org today",
                "Send to support@test.net",
            ],
        })
        register_cache("emails", df)
        
        cmd = r'cache=emails | rex field=text "(?<email>[\w.-]+@[\w.-]+\.\w+)"'
        result = CommandExecutor(cmd).execute()
        
        assert "email" in result.columns
        assert "@" in result["email"].iloc[0]


class TestComplexLogParsing:
    """Tests for complex log parsing scenarios."""

    def test_parse_standard_log_format(self, sample_app_logs):
        """
        Test parsing standard log format.
        
        rex field=_raw "(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})\\s+
        (?<level>\\w+)\\s+(?<logger>[^\\s]+)\\s+-\\s+(?<message>.*)"
        """
        cmd = r'cache=app_logs | rex field=_raw "(?<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?<level>\w+)\s+(?<logger>[^\s]+)\s+-\s+(?<message>.*)"'
        result = CommandExecutor(cmd).execute()
        
        # Should extract all four fields
        assert "timestamp" in result.columns
        assert "level" in result.columns
        assert "logger" in result.columns
        assert "message" in result.columns
        
        # Verify log levels
        valid_levels = {"INFO", "WARN", "ERROR", "DEBUG"}
        assert set(result["level"].dropna().unique()).issubset(valid_levels)

    def test_parse_with_strptime(self, sample_app_logs):
        """
        Test parsing logs and converting timestamp.
        
        rex field=_raw "..." | eval parsed_time=strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        """
        cmd = r'cache=app_logs | rex field=_raw "(?<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})" | eval parsed_time=strptime(timestamp, "%Y-%m-%d %H:%M:%S")'
        result = CommandExecutor(cmd).execute()
        
        assert "timestamp" in result.columns
        assert "parsed_time" in result.columns

    def test_parse_apache_log(self):
        """Test parsing Apache combined log format."""
        logs = [
            '192.168.1.100 - - [01/Jan/2024:10:00:00 +0000] "GET /index.html HTTP/1.1" 200 1234',
            '10.0.0.50 - john [01/Jan/2024:10:01:00 +0000] "POST /api/login HTTP/1.1" 302 0',
            '172.16.0.1 - - [01/Jan/2024:10:02:00 +0000] "GET /favicon.ico HTTP/1.1" 404 0',
        ]
        df = pd.DataFrame({"_raw": logs})
        register_cache("apache_logs", df)
        
        cmd = r'cache=apache_logs | rex field=_raw "(?<client_ip>[\d.]+)\s+\S+\s+(?<user>\S+)\s+\[(?<timestamp>[^\]]+)\]\s+\"(?<request>[^\"]+)\"\s+(?<status>\d+)\s+(?<bytes>\d+)"'
        result = CommandExecutor(cmd).execute()
        
        assert "client_ip" in result.columns
        assert "status" in result.columns
        assert "request" in result.columns

    def test_parse_json_like_logs(self):
        """Test extracting values from JSON-like log messages."""
        logs = [
            '{"user": "john", "action": "login", "result": "success"}',
            '{"user": "jane", "action": "purchase", "result": "failed"}',
            '{"user": "bob", "action": "logout", "result": "success"}',
        ]
        df = pd.DataFrame({"_raw": logs})
        register_cache("json_logs", df)
        
        cmd = r'cache=json_logs | rex field=_raw "\"user\":\s*\"(?<user>[^\"]+)\""'
        result = CommandExecutor(cmd).execute()
        
        assert "user" in result.columns
        assert result["user"].tolist() == ["john", "jane", "bob"]


class TestMultiFieldExtraction:
    """Tests for extracting multiple fields with a single pattern."""

    def test_extract_multiple_named_groups(self):
        """Test extracting multiple fields at once."""
        df = pd.DataFrame({
            "log": [
                "Request: method=GET path=/api/users status=200 duration=150ms",
                "Request: method=POST path=/api/orders status=201 duration=250ms",
                "Request: method=DELETE path=/api/items status=404 duration=50ms",
            ],
        })
        register_cache("request_logs", df)
        
        cmd = r'cache=request_logs | rex field=log "method=(?<method>\w+)\s+path=(?<path>\S+)\s+status=(?<status>\d+)\s+duration=(?<duration>\d+)ms"'
        result = CommandExecutor(cmd).execute()
        
        assert "method" in result.columns
        assert "path" in result.columns
        assert "status" in result.columns
        assert "duration" in result.columns
        
        # Verify values
        assert result["method"].tolist() == ["GET", "POST", "DELETE"]
        assert result["status"].tolist() == ["200", "201", "404"]

    def test_extract_url_components(self):
        """Test extracting components from URLs."""
        df = pd.DataFrame({
            "url": [
                "https://api.example.com:8080/v1/users?id=123",
                "http://localhost:3000/health",
                "https://test.org/api/data?filter=active&limit=10",
            ],
        })
        register_cache("urls", df)
        
        cmd = r'cache=urls | rex field=url "(?<protocol>https?)://(?<host>[^:/]+)(?::(?<port>\d+))?(?<path>/[^?]*)(?:\?(?<query>.*))?"'
        result = CommandExecutor(cmd).execute()
        
        assert "protocol" in result.columns
        assert "host" in result.columns
        assert "path" in result.columns


class TestRexSedMode:
    """Tests for rex sed mode (replacement)."""

    def test_sed_simple_replacement(self):
        """Test simple string replacement with sed mode."""
        df = pd.DataFrame({
            "text": [
                "Hello World",
                "Hello Universe",
                "Hello Everyone",
            ],
        })
        register_cache("greetings", df)
        
        cmd = r'cache=greetings | rex field=text mode=sed "s/Hello/Hi/"'
        result = CommandExecutor(cmd).execute()
        
        assert all("Hi" in t for t in result["text"])

    def test_sed_regex_replacement(self):
        """Test regex replacement with sed mode."""
        df = pd.DataFrame({
            "text": [
                "Phone: 123-456-7890",
                "Contact: 555-123-4567",
                "Call: 999-888-7777",
            ],
        })
        register_cache("phones", df)
        
        cmd = r'cache=phones | rex field=text pattern="\d{3}-\d{3}-\d{4}" mode=sed replacement="[REDACTED]"'
        result = CommandExecutor(cmd).execute()
        
        assert all("[REDACTED]" in t for t in result["text"])

    def test_sed_email_masking(self):
        """Test masking email addresses."""
        df = pd.DataFrame({
            "message": [
                "User email: john@example.com",
                "Contact: jane.doe@company.org",
                "Support: help@test.net",
            ],
        })
        register_cache("messages", df)
        
        cmd = r'cache=messages | rex field=message pattern="[\w.-]+@[\w.-]+\.\w+" mode=sed replacement="[EMAIL]"'
        result = CommandExecutor(cmd).execute()
        
        assert all("[EMAIL]" in m for m in result["message"])


class TestRexWithPipeline:
    """Tests for rex integrated in command pipelines."""

    def test_rex_with_stats(self, sample_app_logs):
        """
        Test rex followed by stats aggregation.
        
        rex ... | stats count by level
        """
        cmd = r'cache=app_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)" | stats count as n by level'
        result = CommandExecutor(cmd).execute()
        
        assert "level" in result.columns
        assert "n" in result.columns
        assert result["n"].sum() <= len(sample_app_logs)

    def test_rex_with_filter(self, sample_app_logs):
        """
        Test rex followed by filter.
        
        rex ... | where level="ERROR"
        """
        cmd = r'cache=app_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)" | filter level="ERROR"'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["level"] == "ERROR")

    def test_rex_with_eval(self, sample_app_logs):
        """
        Test rex followed by eval transformation.
        
        rex ... | eval level_upper=upper(level)
        """
        cmd = r'cache=app_logs | rex field=_raw "(?<level>\w+)\s+com" | eval level_upper=upper(level)'
        result = CommandExecutor(cmd).execute()
        
        assert "level_upper" in result.columns

    def test_complete_log_analysis_pipeline(self, sample_app_logs):
        """
        Complete log analysis pipeline from user examples.
        
        rex field=_raw "..." | eval parsed_time=strptime(timestamp, "%Y-%m-%d %H:%M:%S") |
        stats count by level
        """
        cmd = r'''cache=app_logs | rex field=_raw "(?<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?<level>\w+)\s+(?<logger>[^\s]+)" | stats count as n by level'''
        result = CommandExecutor(cmd).execute()
        
        assert "level" in result.columns
        assert "n" in result.columns


class TestRexEdgeCases:
    """Tests for rex edge cases."""

    def test_rex_no_match(self):
        """Test rex when pattern doesn't match."""
        df = pd.DataFrame({
            "text": [
                "no numbers here",
                "still no numbers",
                "text only",
            ],
        })
        register_cache("no_match", df)
        
        cmd = r'cache=no_match | rex field=text "(?<number>\d+)"'
        result = CommandExecutor(cmd).execute()
        
        # Should have the column but with null values
        assert "number" in result.columns
        assert result["number"].isna().all()

    def test_rex_partial_match(self):
        """Test rex when only some rows match."""
        df = pd.DataFrame({
            "text": [
                "has number 123",
                "no number here",
                "another number 456",
            ],
        })
        register_cache("partial", df)
        
        cmd = r'cache=partial | rex field=text "(?<number>\d+)"'
        result = CommandExecutor(cmd).execute()
        
        assert result["number"].iloc[0] == "123"
        assert pd.isna(result["number"].iloc[1])
        assert result["number"].iloc[2] == "456"

    def test_rex_special_characters(self):
        """Test rex with special characters in pattern."""
        df = pd.DataFrame({
            "log": [
                "Error [E001]: Something failed",
                "Warning [W002]: Check config",
                "Info [I003]: Process started",
            ],
        })
        register_cache("special", df)
        
        cmd = r'cache=special | rex field=log "\[(?<code>[A-Z]\d+)\]"'
        result = CommandExecutor(cmd).execute()
        
        assert "code" in result.columns
        assert result["code"].tolist() == ["E001", "W002", "I003"]

    def test_rex_multiline_content(self):
        """Test rex with content containing newlines."""
        df = pd.DataFrame({
            "text": [
                "Line1\nkey=value1",
                "Header\nkey=value2\nFooter",
                "Start\nkey=value3",
            ],
        })
        register_cache("multiline", df)
        
        cmd = r'cache=multiline | rex field=text "key=(?<value>\w+)"'
        result = CommandExecutor(cmd).execute()
        
        assert "value" in result.columns
        assert result["value"].tolist() == ["value1", "value2", "value3"]


class TestRexErrorHandling:
    """Tests for rex error handling."""

    def test_rex_missing_field(self):
        """Test rex with non-existent field."""
        df = pd.DataFrame({
            "text": ["some text"],
        })
        register_cache("test_data", df)
        
        with pytest.raises(ValueError, match="Field not found"):
            cmd = r'cache=test_data | rex field=nonexistent "(?<x>\w+)"'
            CommandExecutor(cmd).execute()

    def test_rex_invalid_pattern(self):
        """Test rex with invalid regex pattern."""
        df = pd.DataFrame({
            "text": ["some text"],
        })
        register_cache("test_data", df)
        
        with pytest.raises(ValueError, match="Invalid regex"):
            cmd = r'cache=test_data | rex field=text "[invalid(pattern"'
            CommandExecutor(cmd).execute()

