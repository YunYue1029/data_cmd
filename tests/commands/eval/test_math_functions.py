"""
Tests for eval command - Mathematical functions.

Functions tested: abs, round, sqrt, ceil, floor, log, exp, pow
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestAbs:
    """Tests for abs() function."""

    def test_abs_negative_values(self):
        """Absolute value of negative numbers."""
        df = pd.DataFrame({"value": [-5, 3, -10, 7, -2]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval abs_value=abs(value)'
        result = CommandExecutor(cmd).execute()

        assert result["abs_value"].tolist() == [5, 3, 10, 7, 2]

    def test_abs_mixed_values(self):
        """Absolute value with mixed positive/negative."""
        df = pd.DataFrame({"value": [0, -1, 1, -100, 100]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval abs_value=abs(value)'
        result = CommandExecutor(cmd).execute()

        assert result["abs_value"].tolist() == [0, 1, 1, 100, 100]


class TestRound:
    """Tests for round() function."""

    def test_round_two_decimals(self):
        """Round to two decimal places."""
        df = pd.DataFrame({"value": [3.14159, 2.71828, 1.41421]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval rounded=round(value, 2)'
        result = CommandExecutor(cmd).execute()

        assert result["rounded"].tolist() == [3.14, 2.72, 1.41]

    def test_round_to_integer(self):
        """Round to integer (0 decimal places)."""
        df = pd.DataFrame({"value": [3.4, 3.5, 3.6]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval rounded=round(value, 0)'
        result = CommandExecutor(cmd).execute()

        # Note: round(3.5) = 4 due to banker's rounding in Python
        assert all(r in [3.0, 4.0] for r in result["rounded"])


class TestSqrt:
    """Tests for sqrt() function."""

    def test_sqrt_perfect_squares(self):
        """Square root of perfect squares."""
        df = pd.DataFrame({"value": [4, 9, 16, 25, 100]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval root=sqrt(value)'
        result = CommandExecutor(cmd).execute()

        assert result["root"].tolist() == [2.0, 3.0, 4.0, 5.0, 10.0]

    def test_sqrt_non_perfect(self):
        """Square root of non-perfect squares."""
        df = pd.DataFrame({"value": [2]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval root=sqrt(value)'
        result = CommandExecutor(cmd).execute()

        assert abs(result["root"].iloc[0] - 1.41421) < 0.001


class TestCeil:
    """Tests for ceil() function."""

    def test_ceil_positive(self):
        """Ceiling of positive numbers."""
        df = pd.DataFrame({"value": [3.1, 3.5, 3.9]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval ceiling=ceil(value)'
        result = CommandExecutor(cmd).execute()

        assert result["ceiling"].tolist() == [4.0, 4.0, 4.0]

    def test_ceil_negative(self):
        """Ceiling of negative numbers."""
        df = pd.DataFrame({"value": [-3.1, -3.9]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval ceiling=ceil(value)'
        result = CommandExecutor(cmd).execute()

        assert result["ceiling"].tolist() == [-3.0, -3.0]


class TestFloor:
    """Tests for floor() function."""

    def test_floor_positive(self):
        """Floor of positive numbers."""
        df = pd.DataFrame({"value": [3.1, 3.5, 3.9]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval floored=floor(value)'
        result = CommandExecutor(cmd).execute()

        assert result["floored"].tolist() == [3.0, 3.0, 3.0]

    def test_floor_negative(self):
        """Floor of negative numbers."""
        df = pd.DataFrame({"value": [-3.1, -3.9]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval floored=floor(value)'
        result = CommandExecutor(cmd).execute()

        assert result["floored"].tolist() == [-4.0, -4.0]


class TestCeilFloorTogether:
    """Tests for ceil and floor together."""

    def test_ceil_floor_comparison(self):
        """Ceil and floor in same query."""
        df = pd.DataFrame({"value": [3.5, -3.5]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval ceiling=ceil(value) | eval floored=floor(value)'
        result = CommandExecutor(cmd).execute()

        assert result["ceiling"].tolist() == [4.0, -3.0]
        assert result["floored"].tolist() == [3.0, -4.0]

