"""
Tests for join command - Basic join operations.

Join two datasets on a common field
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestInnerJoin:
    """Tests for inner join (default)."""

    def test_join_on_single_field(self):
        """Join two datasets on single field."""
        df1 = pd.DataFrame({
            "user_id": ["U001", "U002", "U003"],
            "name": ["Alice", "Bob", "Carol"],
        })
        df2 = pd.DataFrame({
            "user_id": ["U001", "U002", "U003"],
            "score": [85, 90, 95],
        })
        register_cache("users", df1)
        register_cache("scores", df2)

        cmd = 'cache=users | join user_id [search index="scores"]'
        result = CommandExecutor(cmd).execute()

        assert "name" in result.columns
        assert "score" in result.columns
        assert len(result) == 3

    def test_join_partial_match(self):
        """Join with partial key overlap."""
        df1 = pd.DataFrame({
            "id": ["A", "B", "C"],
            "value1": [1, 2, 3],
        })
        df2 = pd.DataFrame({
            "id": ["B", "C", "D"],
            "value2": [20, 30, 40],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | join id [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        # Join should include matching keys
        assert "value1" in result.columns
        assert "value2" in result.columns


class TestJoinMultipleFields:
    """Tests for join on single field with extra data."""

    def test_join_enriches_data(self):
        """Join enriches data with additional fields."""
        df1 = pd.DataFrame({
            "host": ["web01", "web01", "web02"],
            "requests": [100, 150, 200],
        })
        df2 = pd.DataFrame({
            "host": ["web01", "web02"],
            "region": ["US", "EU"],
        })
        register_cache("metrics", df1)
        register_cache("regions", df2)

        cmd = 'cache=metrics | join host [search index="regions"]'
        result = CommandExecutor(cmd).execute()

        assert "requests" in result.columns
        assert "region" in result.columns


class TestJoinWithDifferentSchemas:
    """Tests for join with different schemas."""

    def test_join_adds_new_columns(self, sample_orders, sample_products):
        """Join adds columns from right dataset."""
        cmd = 'cache=orders | join product_id [search index="products"]'
        result = CommandExecutor(cmd).execute()

        assert "category" in result.columns
        assert "price" in result.columns

    def test_join_preserves_left_columns(self, sample_orders, sample_customers):
        """Join preserves all left dataset columns."""
        cmd = 'cache=orders | join customer_id [search index="customers"]'
        result = CommandExecutor(cmd).execute()

        # Original order columns should be present
        assert "order_id" in result.columns
        assert "amount" in result.columns
        # Joined columns should be present
        assert "segment" in result.columns

