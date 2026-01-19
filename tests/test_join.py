"""
Tests for join command - Subquery and Join operations.

Covers:
- Basic join with subquery
- Nested subqueries with multi-level aggregation
- Multiple joins in a single command
- Join with different aggregation functions
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestBasicJoin:
    """Tests for basic join operations."""

    def test_simple_join_with_stats(self, sample_orders, sample_customers):
        """
        Test basic join with subquery stats.
        
        join customer_id [search index="customers" | stats first(segment) as segment by customer_id]
        """
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment by customer_id]'''
        result = CommandExecutor(cmd).execute()
        
        # Should have all original columns plus segment
        assert "order_id" in result.columns
        assert "customer_id" in result.columns
        assert "segment" in result.columns
        
        # Row count should match original orders
        assert len(result) == len(sample_orders)
        
        # Verify join correctness
        for _, row in result.iterrows():
            customer = sample_customers[sample_customers["customer_id"] == row["customer_id"]]
            if len(customer) > 0:
                assert row["segment"] == customer["segment"].iloc[0]

    def test_join_with_values_aggregation(self, sample_orders, sample_user_info):
        """
        Test join with values() aggregation.
        
        join user_id [search index="user_info" | stats values(department) as dept, values(role) as role by user_id]
        """
        # First register some user events
        user_events = pd.DataFrame({
            "event_id": range(1, 31),
            "user_id": [f"U{i:03d}" for i in np.random.randint(1, 21, 30)],
            "action": ["login", "view", "click"] * 10,
            "status": ["success", "success", "failure"] * 10,
        })
        register_cache("user_events", user_events)
        
        cmd = '''cache=user_events | join user_id [search index="user_info" | stats values(department) as dept, values(role) as role by user_id]'''
        result = CommandExecutor(cmd).execute()
        
        # Should have dept and role columns
        assert "dept" in result.columns
        assert "role" in result.columns
        assert len(result) == len(user_events)

    def test_join_with_stats_count(self, sample_orders, sample_customers):
        """
        Test join followed by stats.
        
        join customer_id [...] | stats count by segment
        """
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id] | stats count by segment'''
        result = CommandExecutor(cmd).execute()
        
        assert "segment" in result.columns
        assert "count" in result.columns
        
        # Total count should match original orders
        assert result["count"].sum() == len(sample_orders)


class TestNestedSubqueries:
    """Tests for nested subqueries and multi-level aggregation."""

    def test_multi_join_with_aggregation(self, sample_orders, sample_customers, sample_products):
        """
        Test multiple joins with aggregation.
        
        join customer_id [search index="customers" | stats values(segment) as segment by customer_id] 
        | join product_id [search index="products" | stats values(category) as category by product_id] 
        | stats sum(amount) as total_sales by segment, category
        """
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment by customer_id] | join product_id [search index="products" | stats first(category) as category by product_id] | stats sum(amount) as total_sales by segment, category'''
        result = CommandExecutor(cmd).execute()
        
        # Should have segment, category, and total_sales
        assert "segment" in result.columns
        assert "category" in result.columns
        assert "total_sales" in result.columns
        
        # Verify total matches original
        assert abs(result["total_sales"].sum() - sample_orders["amount"].sum()) < 0.01

    def test_join_with_region_aggregation(self, sample_orders, sample_customers):
        """
        Test join with multiple fields and region-based aggregation.
        
        join customer_id [...] | stats sum(amount) as total_sales by segment, region, category
        """
        # First, join with customers
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id] | stats sum(amount) as total_sales, count as order_count by segment, region'''
        result = CommandExecutor(cmd).execute()
        
        assert "segment" in result.columns
        assert "region" in result.columns
        assert "total_sales" in result.columns
        assert "order_count" in result.columns
        
        # Total should still match
        assert result["order_count"].sum() == len(sample_orders)


class TestJoinWithComplexSubquery:
    """Tests for join with complex subquery operations."""

    def test_join_with_subquery_filter(self):
        """
        Test join with filtered subquery.
        
        join user_id [search index="user_info" | filter department="Engineering" | stats values(role) as role by user_id]
        """
        # Setup test data
        events = pd.DataFrame({
            "event_id": range(1, 21),
            "user_id": [f"U{i:03d}" for i in [1, 2, 3, 4, 5] * 4],
            "event_type": ["login"] * 20,
        })
        register_cache("events", events)
        
        users = pd.DataFrame({
            "user_id": [f"U{i:03d}" for i in range(1, 6)],
            "department": ["Engineering", "Sales", "Engineering", "Marketing", "Engineering"],
            "role": ["Developer", "Rep", "Lead", "Analyst", "Architect"],
        })
        register_cache("user_info", users)
        
        cmd = '''cache=events | join user_id [search index="user_info" | filter department="Engineering" | stats first(role) as role by user_id]'''
        result = CommandExecutor(cmd).execute()
        
        # Should have role column
        assert "role" in result.columns
        
        # Only users from Engineering should have role values
        eng_users = users[users["department"] == "Engineering"]["user_id"].tolist()
        for _, row in result.iterrows():
            if row["user_id"] in eng_users:
                assert pd.notna(row["role"])

    def test_join_with_calculated_fields(self, sample_orders, sample_products):
        """
        Test join where subquery includes calculated fields.
        
        join product_id [search index="products" | eval price_tier=if(price>100, "high", "low") | stats first(category) as category, first(price_tier) as tier by product_id]
        """
        cmd = '''cache=orders | join product_id [search index="products" | eval price_tier=if(price>100, "high", "low") | stats first(category) as category, first(price_tier) as tier by product_id]'''
        result = CommandExecutor(cmd).execute()
        
        assert "category" in result.columns
        assert "tier" in result.columns
        
        # Verify tier values
        assert set(result["tier"].dropna().unique()).issubset({"high", "low"})


class TestJoinEdgeCases:
    """Tests for edge cases in join operations."""

    def test_join_with_no_matches(self):
        """Test join when no matches exist."""
        left = pd.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        register_cache("left_data", left)
        
        right = pd.DataFrame({
            "id": [4, 5, 6],
            "other_value": ["x", "y", "z"],
        })
        register_cache("right_data", right)
        
        cmd = '''cache=left_data | join id [search index="right_data" | stats first(other_value) as other_value by id]'''
        result = CommandExecutor(cmd).execute()
        
        # Should have all left rows with null other_value
        assert len(result) == 3
        assert "other_value" in result.columns
        assert result["other_value"].isna().all()

    def test_join_with_duplicate_keys(self):
        """Test join when right side has duplicate keys (should use first)."""
        left = pd.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        register_cache("left_data", left)
        
        right = pd.DataFrame({
            "id": [1, 1, 2, 2, 3, 3],
            "other_value": ["x1", "x2", "y1", "y2", "z1", "z2"],
        })
        register_cache("right_data", right)
        
        cmd = '''cache=left_data | join id [search index="right_data" | stats first(other_value) as other_value by id]'''
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 3
        # Should get first value for each id
        assert result[result["id"] == 1]["other_value"].iloc[0] == "x1"

    def test_join_preserves_column_order(self, sample_orders, sample_customers):
        """Test that join preserves original column order before appending new columns."""
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment by customer_id]'''
        result = CommandExecutor(cmd).execute()
        
        # Original columns should come before joined columns
        original_cols = list(sample_orders.columns)
        result_cols = list(result.columns)
        
        for i, col in enumerate(original_cols):
            assert result_cols.index(col) < result_cols.index("segment")


class TestJoinWithStatsAggregation:
    """Tests combining join with various stats aggregations."""

    def test_join_then_full_stats(self, sample_orders, sample_customers):
        """
        Test join followed by comprehensive stats.
        
        join customer_id [...] | stats count by dept, role, status
        """
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id] | stats sum(amount) as total, avg(amount) as avg_amount, count as n by segment, region'''
        result = CommandExecutor(cmd).execute()
        
        # Verify all aggregations present
        assert "total" in result.columns
        assert "avg_amount" in result.columns
        assert "n" in result.columns
        
        # Verify totals
        assert result["n"].sum() == len(sample_orders)
        assert abs(result["total"].sum() - sample_orders["amount"].sum()) < 0.01

    def test_complex_join_aggregation_pipeline(self, sample_orders, sample_customers, sample_products):
        """
        Test complex pipeline: multiple joins + aggregations.
        
        This tests the user's example:
        join customer_id [...] | join product_id [...] | stats sum(amount) as total_sales by segment, region, category
        """
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id] | join product_id [search index="products" | stats first(category) as category by product_id] | stats sum(amount) as total_sales by segment, region, category'''
        result = CommandExecutor(cmd).execute()
        
        # Should have all group fields plus aggregation
        assert "segment" in result.columns
        assert "region" in result.columns
        assert "category" in result.columns
        assert "total_sales" in result.columns
        
        # Total should match
        assert abs(result["total_sales"].sum() - sample_orders["amount"].sum()) < 0.01

