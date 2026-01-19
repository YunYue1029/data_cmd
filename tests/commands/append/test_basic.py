"""
Tests for append command - Basic append operations.

Combine results from multiple queries
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestBasicAppend:
    """Tests for basic append functionality."""

    def test_append_two_datasets(self):
        """Append two datasets together."""
        df1 = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "score": [85, 90],
        })
        df2 = pd.DataFrame({
            "name": ["Carol", "David"],
            "score": [95, 88],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 4
        assert set(result["name"]) == {"Alice", "Bob", "Carol", "David"}

    def test_append_preserves_order(self):
        """Append preserves original data first."""
        df1 = pd.DataFrame({"id": [1, 2]})
        df2 = pd.DataFrame({"id": [3, 4]})
        register_cache("first", df1)
        register_cache("second", df2)

        cmd = 'cache=first | append [search index="second"]'
        result = CommandExecutor(cmd).execute()

        assert result["id"].tolist() == [1, 2, 3, 4]


class TestAppendDifferentColumns:
    """Tests for append with different column sets."""

    def test_append_extra_columns(self):
        """Append with extra columns fills NaN."""
        df1 = pd.DataFrame({
            "name": ["Alice"],
            "age": [30],
        })
        df2 = pd.DataFrame({
            "name": ["Bob"],
            "city": ["NYC"],
        })
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        assert "name" in result.columns
        assert "age" in result.columns
        assert "city" in result.columns

    def test_append_fills_missing_with_nan(self):
        """Missing columns filled with NaN."""
        df1 = pd.DataFrame({"a": [1], "b": [2]})
        df2 = pd.DataFrame({"a": [3], "c": [4]})
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = 'cache=data1 | append [search index="data2"]'
        result = CommandExecutor(cmd).execute()

        # df1's row should have NaN for 'c'
        assert pd.isna(result.iloc[0]["c"])
        # df2's row should have NaN for 'b'
        assert pd.isna(result.iloc[1]["b"])


class TestMultipleAppends:
    """Tests for multiple append operations."""

    def test_chain_two_appends(self):
        """Chain multiple append commands."""
        df1 = pd.DataFrame({"source": ["A"], "count": [10]})
        df2 = pd.DataFrame({"source": ["B"], "count": [20]})
        df3 = pd.DataFrame({"source": ["C"], "count": [30]})
        register_cache("data1", df1)
        register_cache("data2", df2)
        register_cache("data3", df3)

        cmd = 'cache=data1 | append [search index="data2"] | append [search index="data3"]'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3
        assert set(result["source"]) == {"A", "B", "C"}

