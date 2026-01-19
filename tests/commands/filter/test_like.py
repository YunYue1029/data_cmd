"""
Tests for filter/where command - LIKE operator.

Pattern matching with % wildcard
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestLikeStartsWith:
    """Tests for LIKE with prefix matching (pattern%)."""

    def test_like_starts_with(self):
        """Match strings starting with prefix."""
        df = pd.DataFrame({
            "uri": ["/api/users", "/api/orders", "/health", "/api/products"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where uri LIKE "/api%"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3
        assert all(result["uri"].str.startswith("/api"))


class TestLikeEndsWith:
    """Tests for LIKE with suffix matching (%pattern)."""

    def test_like_ends_with(self):
        """Match strings ending with suffix."""
        df = pd.DataFrame({
            "filename": ["report.pdf", "data.csv", "image.pdf", "notes.txt"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where filename LIKE "%.pdf"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["filename"].str.endswith(".pdf"))


class TestLikeContains:
    """Tests for LIKE with contains matching (%pattern%)."""

    def test_like_contains(self):
        """Match strings containing pattern."""
        df = pd.DataFrame({
            "message": ["User logged in", "Failed login attempt", "Password changed", "Login successful"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where message LIKE "%login%"'
        result = CommandExecutor(cmd).execute()

        # Note: LIKE is case-sensitive by default
        assert len(result) >= 1

    def test_like_contains_api(self, sample_web_logs):
        """Filter URIs containing 'api'."""
        cmd = 'cache=web_logs | where uri LIKE "%api%"'
        result = CommandExecutor(cmd).execute()

        assert all(result["uri"].str.contains("api"))


class TestLikeExact:
    """Tests for LIKE with exact matching (no wildcards)."""

    def test_like_exact_match(self):
        """LIKE with exact string (no wildcards)."""
        df = pd.DataFrame({
            "name": ["alice", "bob", "alice"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where name LIKE "alice"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2


class TestLikeCombined:
    """Tests for LIKE combined with other conditions."""

    def test_like_with_and(self, sample_web_logs):
        """LIKE combined with AND condition."""
        cmd = 'cache=web_logs | where uri LIKE "%api%" AND status_code = 200'
        result = CommandExecutor(cmd).execute()

        if len(result) > 0:
            assert all(result["uri"].str.contains("api"))
            assert all(result["status_code"] == 200)

    def test_like_with_or(self):
        """LIKE combined with OR condition."""
        df = pd.DataFrame({
            "path": ["/api/v1", "/api/v2", "/health", "/metrics"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where path LIKE "/api%" OR path LIKE "/health%"'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3

