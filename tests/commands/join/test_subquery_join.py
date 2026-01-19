"""
Tests for join command - Subquery join operations.

Join with computed/filtered subqueries
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestJoinWithSubqueryStats:
    """Tests for join with stats in subquery."""

    def test_join_with_aggregated_subquery(self, sample_orders, sample_customers):
        """Join with aggregated data from subquery."""
        # Calculate order totals by customer
        cmd = '''cache=orders | stats sum(amount) as total_orders by customer_id | join customer_id [search index="customers"]'''
        result = CommandExecutor(cmd).execute()

        assert "total_orders" in result.columns
        assert "segment" in result.columns

    def test_join_baseline_with_current(self, sample_server_metrics):
        """Join baseline stats with current metrics."""
        # First create baseline
        baseline_cmd = 'cache=server_metrics | stats avg(cpu_usage) as baseline_cpu by host'
        baseline = CommandExecutor(baseline_cmd).execute()
        register_cache("baseline", baseline)

        # Get current (simulated)
        current = pd.DataFrame({
            "host": ["server01", "server02", "server03"],
            "current_cpu": [35, 90, 40],
        })
        register_cache("current", current)

        # Join
        cmd = 'cache=baseline | join host [search index="current"]'
        result = CommandExecutor(cmd).execute()

        assert "baseline_cpu" in result.columns
        assert "current_cpu" in result.columns


class TestJoinWithSubqueryFilter:
    """Tests for join with filter in subquery."""

    def test_join_with_pre_filtered_data(self, sample_orders, sample_products):
        """Join with pre-filtered data registered as cache."""
        # Pre-filter electronics products and register
        electronics = sample_products[sample_products["category"] == "Electronics"].copy()
        register_cache("electronics", electronics)

        cmd = 'cache=orders | join product_id [search index="electronics"]'
        result = CommandExecutor(cmd).execute()

        # Should have order columns plus product columns
        assert "order_id" in result.columns
        assert "category" in result.columns


class TestChainedJoins:
    """Tests for chained join operations."""

    def test_multiple_joins(self, sample_orders, sample_products, sample_customers):
        """Multiple joins in sequence."""
        cmd = 'cache=orders | join product_id [search index="products"] | join customer_id [search index="customers"]'
        result = CommandExecutor(cmd).execute()

        # Should have columns from all three tables
        assert "order_id" in result.columns  # from orders
        assert "category" in result.columns  # from products
        assert "segment" in result.columns   # from customers

