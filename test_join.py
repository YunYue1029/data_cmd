"""
Tests for join command.
"""

import pandas as pd
import pytest

from executors import CommandExecutor, register_cache


class TestJoinCommand:
    """Test cases for join command."""

    def test_join_with_subquery(
        self, orders_df: pd.DataFrame, customers_df: pd.DataFrame
    ):
        """Test join with a subquery containing stats aggregation."""
        # Register data
        register_cache("test_data", orders_df)
        register_cache("customers", customers_df)

        # Execute command
        cmd = (
            'cache=test_data | join customer_id '
            '[search index="customers" | stats first(segment) as segment, '
            'first(region) as region by customer_id]'
        )
        result = CommandExecutor(cmd).execute()

        # Manual verification
        subquery_result = customers_df.groupby("customer_id", as_index=False).agg(
            segment=("segment", "first"),
            region=("region", "first")
        )
        expected = orders_df.merge(subquery_result, on="customer_id", how="left")

        # Sort both for comparison
        expected_sorted = expected.sort_values("order_id").reset_index(drop=True)
        result_sorted = result.sort_values("order_id").reset_index(drop=True)

        pd.testing.assert_frame_equal(result_sorted, expected_sorted)

    def test_join_preserves_all_rows(
        self, orders_df: pd.DataFrame, customers_df: pd.DataFrame
    ):
        """Test that join preserves all rows from left table."""
        register_cache("orders", orders_df)
        register_cache("customers", customers_df)

        cmd = (
            'cache=orders | join customer_id '
            '[search index="customers" | select customer_id, segment]'
        )
        result = CommandExecutor(cmd).execute()

        assert len(result) == len(orders_df)
        assert "segment" in result.columns

