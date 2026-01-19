"""
Tests for eval command - Arithmetic operations.

Operations tested: +, -, *, /, parentheses
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestAddition:
    """Tests for addition operator."""

    def test_add_two_fields(self):
        """Add two fields together."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval sum=a+b'
        result = CommandExecutor(cmd).execute()

        assert result["sum"].tolist() == [11, 22, 33]

    def test_add_field_and_constant(self):
        """Add field and constant."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval result=value+10'
        result = CommandExecutor(cmd).execute()

        assert result["result"].tolist() == [11, 12, 13]


class TestSubtraction:
    """Tests for subtraction operator."""

    def test_subtract_two_fields(self, sample_financial_data):
        """Subtract two fields."""
        cmd = 'cache=financial | eval profit=revenue-cost'
        result = CommandExecutor(cmd).execute()

        expected = sample_financial_data["revenue"] - sample_financial_data["cost"]
        pd.testing.assert_series_equal(result["profit"], expected, check_names=False)

    def test_subtract_constant(self):
        """Subtract constant from field."""
        df = pd.DataFrame({"value": [100, 200, 300]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval result=value - 50'
        result = CommandExecutor(cmd).execute()

        # Verify subtraction works (value - 50)
        expected = [50, 150, 250]
        actual = result["result"].tolist()
        assert actual == expected, f"Expected {expected}, got {actual}"


class TestMultiplication:
    """Tests for multiplication operator."""

    def test_multiply_two_fields(self, sample_orders):
        """Multiply two fields."""
        cmd = 'cache=orders | eval total=amount*quantity'
        result = CommandExecutor(cmd).execute()

        expected = sample_orders["amount"] * sample_orders["quantity"]
        pd.testing.assert_series_equal(result["total"], expected, check_names=False)

    def test_multiply_by_constant(self):
        """Multiply field by constant."""
        df = pd.DataFrame({"value": [10, 20, 30]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval doubled=value*2'
        result = CommandExecutor(cmd).execute()

        assert result["doubled"].tolist() == [20, 40, 60]


class TestDivision:
    """Tests for division operator."""

    def test_divide_two_fields(self, sample_financial_data):
        """Divide two fields."""
        cmd = 'cache=financial | eval ratio=revenue/cost'
        result = CommandExecutor(cmd).execute()

        assert "ratio" in result.columns
        assert len(result) == len(sample_financial_data)

    def test_divide_by_constant(self):
        """Divide field by constant."""
        df = pd.DataFrame({"value": [100, 200, 300]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval half=value/2'
        result = CommandExecutor(cmd).execute()

        assert result["half"].tolist() == [50.0, 100.0, 150.0]


class TestParentheses:
    """Tests for parentheses in expressions."""

    def test_parentheses_order_of_operations(self, sample_financial_data):
        """Parentheses affect order of operations."""
        cmd = 'cache=financial | eval margin=(revenue-cost)/revenue'
        result = CommandExecutor(cmd).execute()

        expected = (sample_financial_data["revenue"] - sample_financial_data["cost"]) / sample_financial_data["revenue"]
        pd.testing.assert_series_equal(result["margin"], expected, check_names=False, atol=0.0001)

    def test_nested_parentheses(self):
        """Nested parentheses."""
        df = pd.DataFrame({"a": [10], "b": [5], "c": [2]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval result=((a+b)*c)'
        result = CommandExecutor(cmd).execute()

        assert result["result"].iloc[0] == 30  # (10+5)*2 = 30


class TestChainedEval:
    """Tests for chained eval operations."""

    def test_two_evals(self, sample_financial_data):
        """Chain two eval operations."""
        cmd = 'cache=financial | eval profit=revenue-cost | eval margin=(profit/revenue)*100'
        result = CommandExecutor(cmd).execute()

        assert "profit" in result.columns
        assert "margin" in result.columns

    def test_three_evals(self):
        """Chain three eval operations."""
        df = pd.DataFrame({"value": [100, 200, 300]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval a=value*2 | eval b=a+10 | eval c=b/2'
        result = CommandExecutor(cmd).execute()

        assert result["c"].tolist() == [105.0, 205.0, 305.0]

