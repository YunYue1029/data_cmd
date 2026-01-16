"""
Tests for advanced join operations with subqueries.

Covers:
- Subquery with join
- Nested subqueries with multi-level aggregation
"""

import numpy as np
import pandas as pd
import pytest

from executors import CommandExecutor, register_cache


class TestSubqueryJoin:
    """Test cases for subquery join operations."""

    @pytest.fixture
    def user_events_df(self) -> pd.DataFrame:
        """User activity events."""
        np.random.seed(42)
        n = 200

        return pd.DataFrame({
            "event_id": range(1, n + 1),
            "user_id": np.random.choice([f"U{i:03d}" for i in range(1, 21)], n),
            "status": np.random.choice(["success", "failure", "pending"], n, p=[0.7, 0.2, 0.1]),
            "duration": np.random.uniform(1, 60, n).round(2),
        })

    @pytest.fixture
    def user_info_df(self) -> pd.DataFrame:
        """User information with department and role."""
        return pd.DataFrame({
            "user_id": [f"U{i:03d}" for i in range(1, 21)],
            "department": np.random.choice(
                ["Engineering", "Sales", "Marketing", "Support"], 20
            ),
            "role": np.random.choice(
                ["Admin", "Manager", "Developer", "Analyst"], 20
            ),
        })

    def test_join_with_subquery_and_values(
        self, user_events_df: pd.DataFrame, user_info_df: pd.DataFrame
    ):
        """
        Test: join user_id [search index="user_info" | stats values(department) as dept, 
              values(role) as role by user_id] | stats count by dept, role, status
        """
        register_cache("events", user_events_df)
        register_cache("user_info", user_info_df)

        cmd = (
            'cache=events | join user_id '
            '[search index="user_info" | stats first(department) as dept, '
            'first(role) as role by user_id] '
            '| stats count by dept, role, status'
        )

        result = CommandExecutor(cmd).execute()

        # Verify result structure
        assert "dept" in result.columns
        assert "role" in result.columns
        assert "status" in result.columns
        assert "count" in result.columns

        # Total count should match original events
        assert result["count"].sum() == len(user_events_df)


class TestNestedSubqueryJoin:
    """Test cases for nested subqueries with multi-level aggregation."""

    @pytest.fixture
    def customers_df(self) -> pd.DataFrame:
        """Customer data with segments."""
        return pd.DataFrame({
            "customer_id": [f"C{i:03d}" for i in range(1, 31)],
            "segment": np.random.choice(
                ["Enterprise", "SMB", "Startup", "Individual"], 30
            ),
        })

    @pytest.fixture
    def products_df(self) -> pd.DataFrame:
        """Product data with categories."""
        return pd.DataFrame({
            "product_id": [f"P{i:03d}" for i in range(1, 21)],
            "category": np.random.choice(
                ["Electronics", "Software", "Services", "Hardware"], 20
            ),
        })

    @pytest.fixture
    def sales_df(self) -> pd.DataFrame:
        """Sales transactions."""
        np.random.seed(42)
        n = 500

        return pd.DataFrame({
            "sale_id": range(1, n + 1),
            "customer_id": np.random.choice([f"C{i:03d}" for i in range(1, 31)], n),
            "product_id": np.random.choice([f"P{i:03d}" for i in range(1, 21)], n),
            "region": np.random.choice(["North", "South", "East", "West"], n),
            "amount": np.random.uniform(100, 10000, n).round(2),
        })

    def test_multi_join_with_aggregation(
        self,
        customers_df: pd.DataFrame,
        products_df: pd.DataFrame,
        sales_df: pd.DataFrame,
    ):
        """
        Test: join customer_id [search index="customers" | stats values(segment) as segment by customer_id] 
              | join product_id [search index="products" | stats values(category) as category by product_id] 
              | stats sum(amount) as total_sales by segment, region, category
        """
        register_cache("sales", sales_df)
        register_cache("customers", customers_df)
        register_cache("products", products_df)

        cmd = (
            'cache=sales '
            '| join customer_id [search index="customers" | stats first(segment) as segment by customer_id] '
            '| join product_id [search index="products" | stats first(category) as category by product_id] '
            '| stats sum(amount) as total_sales by segment, region, category'
        )

        result = CommandExecutor(cmd).execute()

        # Verify result structure
        assert "segment" in result.columns
        assert "region" in result.columns
        assert "category" in result.columns
        assert "total_sales" in result.columns

        # Verify total sales approximately matches
        assert result["total_sales"].sum() == pytest.approx(sales_df["amount"].sum(), rel=0.01)

    def test_join_preserves_data_integrity(
        self,
        customers_df: pd.DataFrame,
        sales_df: pd.DataFrame,
    ):
        """Test that join operations preserve data integrity."""
        register_cache("sales", sales_df)
        register_cache("customers", customers_df)

        cmd = (
            'cache=sales '
            '| join customer_id [search index="customers" | select customer_id, segment]'
        )

        result = CommandExecutor(cmd).execute()

        # All original sales should be present (left join)
        assert len(result) == len(sales_df)

        # Segment column should be added
        assert "segment" in result.columns

        # All customer segments should be valid
        valid_segments = set(customers_df["segment"].unique()) | {None}
        assert set(result["segment"].dropna().unique()).issubset(
            set(customers_df["segment"].unique())
        )

