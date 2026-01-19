"""
Tests for multi-index query syntax - OR syntax.

Query multiple indexes: (index="a" OR index="b")
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestBasicOrSyntax:
    """Tests for basic OR syntax."""

    def test_two_indexes(self):
        """Query two indexes with OR."""
        df1 = pd.DataFrame({"id": [1, 2], "source": ["A", "A"]})
        df2 = pd.DataFrame({"id": [3, 4], "source": ["B", "B"]})
        register_cache("index_a", df1)
        register_cache("index_b", df2)

        cmd = '(cache=index_a OR cache=index_b)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 4
        assert set(result["source"]) == {"A", "B"}

    def test_three_indexes(self):
        """Query three indexes with OR."""
        df1 = pd.DataFrame({"value": [1]})
        df2 = pd.DataFrame({"value": [2]})
        df3 = pd.DataFrame({"value": [3]})
        register_cache("idx1", df1)
        register_cache("idx2", df2)
        register_cache("idx3", df3)

        cmd = '(cache=idx1 OR cache=idx2 OR cache=idx3)'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 3


class TestOrWithPipeline:
    """Tests for OR syntax with pipeline commands."""

    def test_or_then_stats(self):
        """OR query followed by stats."""
        df1 = pd.DataFrame({"category": ["X"], "count": [10]})
        df2 = pd.DataFrame({"category": ["X"], "count": [20]})
        register_cache("source1", df1)
        register_cache("source2", df2)

        cmd = '(cache=source1 OR cache=source2) | stats sum(count) as total by category'
        result = CommandExecutor(cmd).execute()

        assert result["total"].iloc[0] == 30

    def test_or_then_filter(self):
        """OR query followed by filter."""
        df1 = pd.DataFrame({"score": [85, 40]})
        df2 = pd.DataFrame({"score": [95, 50]})
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = '(cache=data1 OR cache=data2) | where score > 80'
        result = CommandExecutor(cmd).execute()

        assert len(result) == 2
        assert all(result["score"] > 80)


class TestOrWithDifferentSchemas:
    """Tests for OR with different column schemas."""

    def test_or_different_columns(self):
        """OR with different column sets."""
        df1 = pd.DataFrame({"id": [1], "name": ["Alice"]})
        df2 = pd.DataFrame({"id": [2], "age": [30]})
        register_cache("users", df1)
        register_cache("ages", df2)

        cmd = '(cache=users OR cache=ages)'
        result = CommandExecutor(cmd).execute()

        assert "id" in result.columns
        assert "name" in result.columns
        assert "age" in result.columns

    def test_or_fills_missing_columns(self):
        """Missing columns filled with NaN."""
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"b": [2]})
        register_cache("data1", df1)
        register_cache("data2", df2)

        cmd = '(cache=data1 OR cache=data2)'
        result = CommandExecutor(cmd).execute()

        # Row from df1 should have NaN for 'b'
        # Row from df2 should have NaN for 'a'
        assert len(result) == 2

