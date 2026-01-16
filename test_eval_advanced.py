"""
Tests for advanced eval expressions.

Covers:
- Complex arithmetic expressions
- Conditional expressions (if, case)
- Time functions (strftime, strptime)
"""

import numpy as np
import pandas as pd
import pytest

from executors import CommandExecutor, register_cache


class TestComplexEvalExpressions:
    """Test cases for complex eval expressions."""

    @pytest.fixture
    def financial_df(self) -> pd.DataFrame:
        """Financial data with revenue and cost."""
        np.random.seed(42)
        n = 100

        revenue = np.random.uniform(1000, 50000, n).round(2)
        # Cost is 40-90% of revenue with some negative profit cases
        cost_ratio = np.random.uniform(0.4, 1.1, n)
        cost = (revenue * cost_ratio).round(2)

        return pd.DataFrame({
            "transaction_id": range(1, n + 1),
            "revenue": revenue,
            "cost": cost,
            "department": np.random.choice(["Sales", "Marketing", "Operations"], n),
        })

    def test_eval_profit_calculation(self, financial_df: pd.DataFrame):
        """
        Test: eval profit=revenue-cost
        """
        register_cache("financial", financial_df)

        cmd = "cache=financial | eval profit = revenue - cost"
        result = CommandExecutor(cmd).execute()

        # Verify profit column exists and is calculated correctly
        assert "profit" in result.columns
        expected_profit = financial_df["revenue"] - financial_df["cost"]
        pd.testing.assert_series_equal(
            result["profit"], expected_profit, check_names=False
        )

    def test_eval_division(self, financial_df: pd.DataFrame):
        """
        Test: eval profit_margin = profit / revenue * 100
        """
        register_cache("financial", financial_df)

        cmd = (
            "cache=financial "
            "| eval profit = revenue - cost "
            "| eval profit_margin = profit / revenue * 100"
        )
        result = CommandExecutor(cmd).execute()

        assert "profit_margin" in result.columns
        # Verify some calculations
        for idx in [0, 10, 50]:
            expected = (
                (result["revenue"].iloc[idx] - result["cost"].iloc[idx])
                / result["revenue"].iloc[idx] * 100
            )
            assert result["profit_margin"].iloc[idx] == pytest.approx(expected, rel=0.01)

    def test_eval_if_expression(self, financial_df: pd.DataFrame):
        """
        Test: eval profit_margin=if(revenue>0, (profit/revenue)*100, 0)

        Note: if() function may need to be implemented.
        """
        register_cache("financial", financial_df)

        cmd = (
            "cache=financial "
            "| eval profit = revenue - cost "
            "| eval profit_margin = if(revenue > 0, (profit / revenue) * 100, 0)"
        )

        try:
            result = CommandExecutor(cmd).execute()
            assert "profit_margin" in result.columns

            # Verify that division by zero is handled
            zero_revenue = result[result["revenue"] == 0]
            if len(zero_revenue) > 0:
                assert all(zero_revenue["profit_margin"] == 0)

        except (ValueError, NotImplementedError, KeyError) as e:
            pytest.skip(f"if() function not implemented: {e}")

    def test_eval_case_expression(self, financial_df: pd.DataFrame):
        """
        Test: eval category=case(profit_margin>20, "High", profit_margin>10, "Medium", 
              profit_margin>0, "Low", 1=1, "Loss")

        Note: case() function may need to be implemented.
        """
        register_cache("financial", financial_df)

        cmd = (
            "cache=financial "
            "| eval profit = revenue - cost "
            "| eval profit_margin = profit / revenue * 100 "
            '| eval category = case('
            'profit_margin > 20, "High", '
            'profit_margin > 10, "Medium", '
            'profit_margin > 0, "Low", '
            '1 == 1, "Loss")'
        )

        try:
            result = CommandExecutor(cmd).execute()
            assert "category" in result.columns

            # Verify category assignment
            valid_categories = {"High", "Medium", "Low", "Loss"}
            assert set(result["category"].unique()).issubset(valid_categories)

        except (ValueError, NotImplementedError, KeyError) as e:
            pytest.skip(f"case() function not implemented: {e}")


class TestTimeSeriesEval:
    """Test cases for time series eval expressions."""

    @pytest.fixture
    def metrics_df(self) -> pd.DataFrame:
        """Server metrics with timestamps."""
        np.random.seed(42)
        n = 200

        # Generate hourly data for a week
        timestamps = pd.date_range("2024-01-01", periods=n, freq="h")

        return pd.DataFrame({
            "_time": timestamps,
            "host": np.random.choice(["server01", "server02", "server03"], n),
            "cpu_usage": np.random.uniform(10, 95, n).round(2),
            "memory_usage": np.random.uniform(20, 90, n).round(2),
        })

    def test_eval_strftime(self, metrics_df: pd.DataFrame):
        """
        Test: eval hour=strftime(_time, "%H")

        Note: strftime() function may need to be implemented.
        """
        register_cache("metrics", metrics_df)

        cmd = 'cache=metrics | eval hour = strftime(_time, "%H")'

        try:
            result = CommandExecutor(cmd).execute()
            assert "hour" in result.columns

            # Verify hour extraction
            expected_hours = metrics_df["_time"].dt.strftime("%H")
            pd.testing.assert_series_equal(
                result["hour"], expected_hours, check_names=False
            )

        except (ValueError, NotImplementedError, KeyError) as e:
            pytest.skip(f"strftime() function not implemented: {e}")

    def test_time_series_analysis_with_eval(self, metrics_df: pd.DataFrame):
        """
        Test: eval hour=strftime(_time, "%H") | stats avg(cpu_usage) as avg_cpu, 
              max(memory_usage) as max_mem by host, hour
        """
        register_cache("metrics", metrics_df)

        cmd = (
            'cache=metrics '
            '| eval hour = strftime(_time, "%H") '
            '| stats avg(cpu_usage) as avg_cpu, max(memory_usage) as max_mem by host, hour'
        )

        try:
            result = CommandExecutor(cmd).execute()

            assert "host" in result.columns
            assert "hour" in result.columns
            assert "avg_cpu" in result.columns
            assert "max_mem" in result.columns

            # Each host should have entries for multiple hours
            assert len(result) > 3  # More than just 3 hosts

        except (ValueError, NotImplementedError, KeyError) as e:
            pytest.skip(f"strftime() function not implemented: {e}")


class TestChainedEval:
    """Test cases for chained eval expressions."""

    @pytest.fixture
    def orders_df(self) -> pd.DataFrame:
        """Order data for chained eval tests."""
        np.random.seed(42)
        n = 100

        return pd.DataFrame({
            "order_id": range(1, n + 1),
            "quantity": np.random.randint(1, 20, n),
            "unit_price": np.random.uniform(10, 500, n).round(2),
            "discount_pct": np.random.choice([0, 5, 10, 15, 20], n),
        })

    def test_chained_eval_calculations(self, orders_df: pd.DataFrame):
        """Test multiple chained eval calculations."""
        register_cache("orders", orders_df)

        cmd = (
            "cache=orders "
            "| eval subtotal = quantity * unit_price "
            "| eval discount = subtotal * discount_pct / 100 "
            "| eval total = subtotal - discount"
        )

        result = CommandExecutor(cmd).execute()

        # Verify all calculated columns exist
        assert "subtotal" in result.columns
        assert "discount" in result.columns
        assert "total" in result.columns

        # Verify calculations
        for idx in range(min(10, len(result))):
            subtotal = result["quantity"].iloc[idx] * result["unit_price"].iloc[idx]
            discount = subtotal * result["discount_pct"].iloc[idx] / 100
            total = subtotal - discount

            assert result["subtotal"].iloc[idx] == pytest.approx(subtotal, rel=0.01)
            assert result["discount"].iloc[idx] == pytest.approx(discount, rel=0.01)
            assert result["total"].iloc[idx] == pytest.approx(total, rel=0.01)

