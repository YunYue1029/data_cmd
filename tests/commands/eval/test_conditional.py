"""
Tests for eval command - Conditional expressions.

Functions tested: if, case
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestIfBasic:
    """Tests for basic if() function."""

    def test_if_simple_comparison(self):
        """Simple if with comparison."""
        df = pd.DataFrame({"value": [10, 50, 100]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval category=if(value > 50, "high", "low")'
        result = CommandExecutor(cmd).execute()

        assert result["category"].tolist() == ["low", "low", "high"]

    def test_if_equality(self):
        """If with equality check."""
        df = pd.DataFrame({"status": ["active", "inactive", "active"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval is_active=if(status = "active", 1, 0)'
        result = CommandExecutor(cmd).execute()

        assert result["is_active"].tolist() == [1, 0, 1]

    def test_if_with_fields(self, sample_financial_data):
        """If comparing fields."""
        cmd = 'cache=financial | eval profit=revenue-cost | eval profitable=if(profit > 0, "yes", "no")'
        result = CommandExecutor(cmd).execute()

        assert "profitable" in result.columns


class TestIfWithDivision:
    """Tests for if() with division (zero handling)."""

    def test_if_zero_division_handling(self):
        """If to handle zero division."""
        df = pd.DataFrame({
            "revenue": [100, 0, 200],
            "cost": [50, 25, 100],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval profit=revenue-cost | eval margin=if(revenue>0, (profit/revenue)*100, 0)'
        result = CommandExecutor(cmd).execute()

        # Where revenue=0, margin should be 0
        assert result[result["revenue"] == 0]["margin"].iloc[0] == 0


class TestNestedIf:
    """Tests for nested if() expressions."""

    def test_nested_if_two_levels(self):
        """Nested if with two levels."""
        df = pd.DataFrame({
            "status": ["active", "inactive", "pending"],
            "count": [10, 5, 3],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval priority=if(status="active", if(count>5, "high", "medium"), "low")'
        result = CommandExecutor(cmd).execute()

        assert result[result["status"] == "active"]["priority"].iloc[0] == "high"
        assert result[result["status"] == "inactive"]["priority"].iloc[0] == "low"


class TestCaseBasic:
    """Tests for basic case() function."""

    def test_case_simple_grades(self):
        """Case for grade assignment."""
        df = pd.DataFrame({"score": [95, 85, 75, 65, 55]})
        register_cache("scores", df)

        cmd = 'cache=scores | eval grade=case(score>=90, "A", score>=80, "B", score>=70, "C", score>=60, "D", 1=1, "F")'
        result = CommandExecutor(cmd).execute()

        assert result["grade"].tolist() == ["A", "B", "C", "D", "F"]

    def test_case_with_default(self):
        """Case with default value (1=1)."""
        df = pd.DataFrame({"value": [1, 5, 10, 15]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval category=case(value>10, "high", value>5, "medium", 1=1, "low")'
        result = CommandExecutor(cmd).execute()

        assert result["category"].tolist() == ["low", "low", "medium", "high"]


class TestCaseWithFields:
    """Tests for case() with field comparisons."""

    def test_case_profit_categories(self, sample_financial_data):
        """Case for categorizing profit margins."""
        cmd = '''cache=financial | eval profit=revenue-cost | eval margin=if(revenue>0, (profit/revenue)*100, 0) | eval category=case(margin>20, "High", margin>10, "Medium", margin>0, "Low", 1=1, "Loss")'''
        result = CommandExecutor(cmd).execute()

        valid_categories = {"High", "Medium", "Low", "Loss"}
        assert set(result["category"].unique()).issubset(valid_categories)


class TestBooleanExpressions:
    """Tests for boolean expressions in if/case."""

    def test_if_with_and(self):
        """If with AND condition."""
        df = pd.DataFrame({
            "cpu": [30, 80, 90],
            "memory": [40, 85, 70],
        })
        register_cache("metrics", df)

        cmd = 'cache=metrics | eval critical=if(cpu>70 AND memory>70, 1, 0)'
        result = CommandExecutor(cmd).execute()

        assert result["critical"].tolist() == [0, 1, 0]

    def test_if_with_or(self):
        """If with OR condition."""
        df = pd.DataFrame({
            "status": ["error", "ok", "warning"],
        })
        register_cache("logs", df)

        cmd = 'cache=logs | eval alert=if(status="error" OR status="warning", 1, 0)'
        result = CommandExecutor(cmd).execute()

        assert result["alert"].tolist() == [1, 0, 1]

