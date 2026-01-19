"""
Tests for filter/where command - Boolean operators.

Operators tested: AND, OR, NOT, parentheses grouping
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestAnd:
    """Tests for AND operator."""

    def test_and_two_conditions(self):
        """Filter with two AND conditions."""
        df = pd.DataFrame({
            "status": ["active", "active", "inactive", "active"],
            "score": [80, 40, 90, 60],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where status = "active" AND score > 50'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["status"] == "active")
        assert all(result["score"] > 50)

    def test_and_three_conditions(self):
        """Filter with three AND conditions."""
        df = pd.DataFrame({
            "a": [1, 1, 1, 0],
            "b": [1, 1, 0, 1],
            "c": [1, 0, 1, 1],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where a = 1 AND b = 1 AND c = 1'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 1


class TestOr:
    """Tests for OR operator."""

    def test_or_two_conditions(self):
        """Filter with two OR conditions."""
        df = pd.DataFrame({
            "status": ["error", "ok", "warning", "ok"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where status = "error" OR status = "warning"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert set(result["status"]) == {"error", "warning"}

    def test_or_numeric_conditions(self):
        """Filter with OR on numeric values."""
        df = pd.DataFrame({
            "code": [200, 201, 400, 404, 500],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where code = 200 OR code = 201'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2


class TestNot:
    """Tests for NOT operator."""

    def test_not_simple(self):
        """Filter with NOT condition."""
        df = pd.DataFrame({
            "active": [1, 0, 1, 0, 1],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where NOT active = 1'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["active"] == 0)


class TestParentheses:
    """Tests for parentheses grouping."""

    def test_parentheses_or_and(self):
        """Parentheses to group OR conditions before AND."""
        df = pd.DataFrame({
            "host": ["web01", "web02", "db01", "web01"],
            "status": ["ok", "ok", "ok", "error"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where (host = "web01" OR host = "web02") AND status = "ok"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["status"] == "ok")
        assert all(result["host"].isin(["web01", "web02"]))

    def test_nested_parentheses(self):
        """Nested parentheses in filter."""
        df = pd.DataFrame({
            "a": [1, 0, 1, 0],
            "b": [1, 1, 0, 0],
            "c": [1, 1, 1, 0],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where (a = 1 AND b = 1) OR c = 1'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3


class TestComplexBooleanExpressions:
    """Tests for complex boolean expressions."""

    def test_mixed_and_or(self, sample_web_logs):
        """Complex mix of AND and OR."""
        cmd = 'cache=web_logs | where (status_code = 200 OR status_code = 201) AND host = "web01"'
        result = CommandExecutor(cmd).execute()

        if len(result) > 0:
            assert all(result["status_code"].isin([200, 201]))
            assert all(result["host"] == "web01")

    def test_error_status_codes(self, sample_web_logs):
        """Filter for 4xx and 5xx status codes."""
        cmd = 'cache=web_logs | where status_code >= 400'
        result = CommandExecutor(cmd).execute()

        assert all(result["status_code"] >= 400)

