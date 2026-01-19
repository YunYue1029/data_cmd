"""
Tests for transaction command - Basic transaction grouping.

Group related events by field(s)
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestBasicGrouping:
    """Tests for basic transaction grouping."""

    def test_group_by_session_id(self, sample_session_logs):
        """Group events by session ID."""
        cmd = 'cache=session_logs | transaction session_id'
        result = CommandExecutor(cmd).execute()

        # Should have unique sessions
        unique_sessions = sample_session_logs["session_id"].nunique()
        assert len(result) == unique_sessions

    def test_group_by_user(self, sample_session_logs):
        """Group events by user - may produce multiple transactions per user."""
        cmd = 'cache=session_logs | transaction user_id'
        result = CommandExecutor(cmd).execute()

        # At least one transaction per unique user
        unique_users = sample_session_logs["user_id"].nunique()
        assert len(result) >= unique_users


class TestTransactionWithMultipleFields:
    """Tests for transaction with multiple grouping fields."""

    def test_group_by_user_and_session(self, sample_session_logs):
        """Group by user and session."""
        cmd = 'cache=session_logs | transaction user_id, session_id'
        result = CommandExecutor(cmd).execute()

        # Should produce unique combinations
        assert len(result) > 0


class TestTransactionMetadata:
    """Tests for transaction metadata fields."""

    def test_transaction_creates_duration(self, sample_session_logs):
        """Transaction creates duration field."""
        cmd = 'cache=session_logs | transaction session_id'
        result = CommandExecutor(cmd).execute()

        assert "duration" in result.columns

    def test_transaction_creates_event_count(self, sample_session_logs):
        """Transaction creates event_count field."""
        cmd = 'cache=session_logs | transaction session_id'
        result = CommandExecutor(cmd).execute()

        assert "event_count" in result.columns
        # Each transaction should have at least 1 event
        assert all(result["event_count"] >= 1)

