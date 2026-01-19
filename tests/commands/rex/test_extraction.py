"""
Tests for rex command - Basic regex extraction.

Field extraction using regular expressions
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestBasicExtraction:
    """Tests for basic regex extraction."""

    def test_extract_numbers(self):
        """Extract numbers from text."""
        df = pd.DataFrame({
            "text": ["order 123", "order 456", "order 789"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=text "order (?<order_num>\\d+)"'
        result = CommandExecutor(cmd).execute()

        assert "order_num" in result.columns
        assert result["order_num"].tolist() == ["123", "456", "789"]

    def test_extract_from_log(self, sample_app_logs):
        """Extract log level from application logs."""
        cmd = 'cache=app_logs | rex field=_raw "(?<level>INFO|WARN|ERROR|DEBUG)"'
        result = CommandExecutor(cmd).execute()

        assert "level" in result.columns
        valid_levels = {"INFO", "WARN", "ERROR", "DEBUG"}
        extracted = set(result["level"].dropna().unique())
        assert extracted.issubset(valid_levels)


class TestMultipleCaptures:
    """Tests for extracting multiple fields."""

    def test_extract_two_fields(self):
        """Extract two fields from text."""
        df = pd.DataFrame({
            "log": ["user=alice action=login", "user=bob action=logout"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=log "user=(?<username>\\w+) action=(?<action>\\w+)"'
        result = CommandExecutor(cmd).execute()

        assert "username" in result.columns
        assert "action" in result.columns
        assert result["username"].tolist() == ["alice", "bob"]
        assert result["action"].tolist() == ["login", "logout"]

    def test_extract_ip_and_port(self):
        """Extract IP address and port."""
        df = pd.DataFrame({
            "connection": ["connected to 192.168.1.1:8080", "connected to 10.0.0.1:443"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=connection "(?<ip>\\d+\\.\\d+\\.\\d+\\.\\d+):(?<port>\\d+)"'
        result = CommandExecutor(cmd).execute()

        assert "ip" in result.columns
        assert "port" in result.columns


class TestPartialMatch:
    """Tests for partial matches (not all rows match)."""

    def test_extract_with_no_match(self):
        """Handle rows that don't match pattern."""
        df = pd.DataFrame({
            "text": ["error: 123", "warning only", "error: 456"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=text "error: (?<code>\\d+)"'
        result = CommandExecutor(cmd).execute()

        assert "code" in result.columns
        # Non-matching rows should have NaN
        assert result["code"].iloc[0] == "123"
        assert pd.isna(result["code"].iloc[1])
        assert result["code"].iloc[2] == "456"

