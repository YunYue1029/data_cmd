"""
Tests for transaction command - Event grouping into sessions/transactions.

Covers:
- Basic transaction grouping
- Transaction with maxspan parameter
- Transaction with multiple by fields
- Session analysis use cases
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache


class TestBasicTransaction:
    """Tests for basic transaction operations."""

    def test_transaction_single_user(self):
        """Group events into transactions for a single user."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(seconds=30),
                base_time + timedelta(minutes=2),
                base_time + timedelta(minutes=10),  # New session
                base_time + timedelta(minutes=11),
            ],
            "user_id": ["U001"] * 5,
            "action": ["login", "view", "click", "login", "view"],
        })
        register_cache("user_events", df)

        cmd = 'cache=user_events | transaction user_id maxspan=5m'
        result = CommandExecutor(cmd).execute()

        # Should have 2 transactions (first 3 events, then 2 events)
        assert len(result) == 2
        assert "duration" in result.columns
        assert "event_count" in result.columns

    def test_transaction_multiple_users(self):
        """Group events into transactions for multiple users."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(seconds=30),
                base_time + timedelta(seconds=10),
                base_time + timedelta(seconds=40),
            ],
            "user_id": ["U001", "U001", "U002", "U002"],
            "action": ["login", "view", "login", "click"],
        })
        register_cache("user_events", df)

        cmd = 'cache=user_events | transaction user_id maxspan=5m'
        result = CommandExecutor(cmd).execute()

        # Should have 2 transactions (one per user)
        assert len(result) == 2


class TestTransactionMaxspan:
    """Tests for transaction maxspan parameter."""

    def test_maxspan_1_minute(self):
        """Test maxspan of 1 minute - gap between consecutive events must exceed maxspan."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(seconds=30),  # 30s from first -> within 1m
                base_time + timedelta(minutes=2),   # 90s from second -> beyond 1m -> new txn
                base_time + timedelta(minutes=2, seconds=30),  # 30s from third -> within 1m
            ],
            "user_id": ["U001"] * 4,
            "event": ["a", "b", "c", "d"],
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user_id maxspan=1m'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2  # Two transactions

    def test_maxspan_30_minutes(self):
        """Test maxspan of 30 minutes - gap between consecutive events must exceed maxspan."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(minutes=10),   # 10m gap -> within 30m
                base_time + timedelta(minutes=20),   # 10m gap -> within 30m
                base_time + timedelta(minutes=55),   # 35m gap -> beyond 30m -> new txn
            ],
            "user_id": ["U001"] * 4,
            "event": ["a", "b", "c", "d"],
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user_id maxspan=30m'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2

    def test_maxspan_1_hour(self):
        """Test maxspan of 1 hour."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(minutes=30),
                base_time + timedelta(minutes=50),
                base_time + timedelta(hours=2),  # Beyond 1h -> new txn
            ],
            "user_id": ["U001"] * 4,
            "event": ["a", "b", "c", "d"],
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user_id maxspan=1h'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2


class TestTransactionFields:
    """Tests for transaction field outputs."""

    def test_transaction_duration_calculation(self):
        """Verify duration is correctly calculated."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time,
                base_time + timedelta(seconds=60),  # 60 seconds later
            ],
            "user_id": ["U001", "U001"],
            "event": ["start", "end"],
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user_id maxspan=5m'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 1
        # Duration should be 60 seconds
        assert result["duration"].iloc[0] == pytest.approx(60, abs=1)

    def test_transaction_event_count(self):
        """Verify event_count is correctly calculated."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        df = pd.DataFrame({
            "_time": [
                base_time + timedelta(seconds=i * 10)
                for i in range(5)
            ],
            "user_id": ["U001"] * 5,
            "event": ["a", "b", "c", "d", "e"],
        })
        register_cache("events", df)

        cmd = 'cache=events | transaction user_id maxspan=5m'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 1
        assert result["event_count"].iloc[0] == 5


class TestTransactionWithStats:
    """Tests for transaction followed by stats."""

    def test_transaction_with_session_stats(self, sample_user_events):
        """Calculate session statistics after transaction grouping."""
        cmd = 'cache=user_events | transaction user_id maxspan=5m | stats avg(duration) as avg_session_duration, avg(event_count) as avg_events by user_id'
        result = CommandExecutor(cmd).execute()

        assert "user_id" in result.columns
        assert "avg_session_duration" in result.columns
        assert "avg_events" in result.columns

    def test_transaction_overall_stats(self, sample_user_events):
        """Calculate overall transaction statistics."""
        cmd = 'cache=user_events | transaction user_id maxspan=5m | stats count as total_sessions, avg(duration) as avg_duration, avg(event_count) as avg_events'
        result = CommandExecutor(cmd).execute()

        assert "total_sessions" in result.columns
        assert "avg_duration" in result.columns
        assert "avg_events" in result.columns


class TestTransactionSessionAnalysis:
    """Tests for session analysis use cases."""

    def test_web_session_analysis(self):
        """Analyze web browsing sessions."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Create realistic session data
        data = []
        for user_num in range(3):
            user_id = f"U{user_num:03d}"
            for session in range(2):  # 2 sessions per user
                session_start = base_time + timedelta(hours=user_num + session * 2)
                for event in range(4):  # 4 events per session
                    data.append({
                        "_time": session_start + timedelta(seconds=event * 30),
                        "user_id": user_id,
                        "page": f"/page{event}",
                    })
        
        df = pd.DataFrame(data)
        register_cache("page_views", df)

        cmd = 'cache=page_views | transaction user_id maxspan=5m | stats count as sessions, avg(event_count) as avg_pages by user_id'
        result = CommandExecutor(cmd).execute()

        assert "user_id" in result.columns
        assert "sessions" in result.columns
        # Each user should have 2 sessions
        for _, row in result.iterrows():
            assert row["sessions"] == 2

    def test_user_journey_analysis(self, sample_user_events):
        """Analyze user journeys across sessions."""
        # Get session data
        cmd1 = 'cache=user_events | transaction user_id maxspan=5m'
        sessions = CommandExecutor(cmd1).execute()
        register_cache("sessions", sessions)

        # Analyze session metrics
        cmd2 = 'cache=sessions | stats count as total_sessions, sum(event_count) as total_events, avg(duration) as avg_duration by user_id'
        result = CommandExecutor(cmd2).execute()

        assert "user_id" in result.columns
        assert "total_sessions" in result.columns
        assert "total_events" in result.columns

