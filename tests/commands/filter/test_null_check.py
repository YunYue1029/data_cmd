"""
Tests for filter/where command - Null check functions.

Functions tested: isnull(), isnotnull()
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestIsnullFilter:
    """Tests for isnull() in filter."""

    def test_isnull_filter(self):
        """Filter rows where field is null."""
        df = pd.DataFrame({
            "value": [1, None, 3, None, 5],
            "name": ["a", "b", "c", "d", "e"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where isnull(value)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(pd.isna(result["value"]))

    def test_isnull_string_field(self):
        """Filter rows where string field is null."""
        df = pd.DataFrame({
            "name": ["Alice", None, "Bob", None],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where isnull(name)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2


class TestIsnotnullFilter:
    """Tests for isnotnull() in filter."""

    def test_isnotnull_filter(self):
        """Filter rows where field is not null."""
        df = pd.DataFrame({
            "value": [1, None, 3, None, 5],
            "name": ["a", "b", "c", "d", "e"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where isnotnull(value)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3
        assert all(pd.notna(result["value"]))

    def test_isnotnull_all_present(self):
        """Filter when all values are present."""
        df = pd.DataFrame({
            "value": [1, 2, 3],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where isnotnull(value)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3


class TestNullCheckCombined:
    """Tests for null checks combined with other conditions."""

    def test_isnotnull_and_comparison(self):
        """isnotnull combined with value comparison."""
        df = pd.DataFrame({
            "value": [10, None, 30, None, 50],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | where isnotnull(value) AND value > 20'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["value"] > 20)

