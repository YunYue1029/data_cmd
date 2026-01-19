"""
Tests for filter/where command - IN and NOT IN operators.

List membership filtering
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestIn:
    """Tests for IN operator."""

    def test_in_string_list(self):
        """Filter by string list membership."""
        df = pd.DataFrame({
            "status": ["active", "inactive", "pending", "deleted", "active"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where status IN ("active", "pending")'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3
        assert all(result["status"].isin(["active", "pending"]))

    def test_in_numeric_list(self):
        """Filter by numeric list membership."""
        df = pd.DataFrame({
            "code": [200, 201, 400, 404, 500],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where code IN (200, 201)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["code"].isin([200, 201]))

    def test_in_single_value(self):
        """IN with single value (equivalent to =)."""
        df = pd.DataFrame({
            "value": [1, 2, 3, 1],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where value IN (1)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2


class TestNotIn:
    """Tests for NOT IN operator."""

    def test_not_in_string_list(self):
        """Filter by string list exclusion."""
        df = pd.DataFrame({
            "status": ["active", "inactive", "pending", "deleted"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where status NOT IN ("inactive", "deleted")'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(~result["status"].isin(["inactive", "deleted"]))

    def test_not_in_numeric_list(self):
        """Filter by numeric list exclusion."""
        df = pd.DataFrame({
            "code": [200, 201, 400, 404, 500],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where code NOT IN (400, 404, 500)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["code"].isin([200, 201]))


class TestInCombined:
    """Tests for IN combined with other conditions."""

    def test_in_with_and(self):
        """IN combined with AND condition."""
        df = pd.DataFrame({
            "host": ["web01", "web02", "db01"],
            "status": ["ok", "ok", "ok"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where host IN ("web01", "web02") AND status = "ok"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2

    def test_in_practical_status_codes(self, sample_web_logs):
        """Practical: filter success status codes."""
        cmd = 'cache=web_logs | where status_code IN (200, 201, 204)'
        result = CommandExecutor(cmd).execute()

        if len(result) > 0:
            assert all(result["status_code"].isin([200, 201, 204]))

