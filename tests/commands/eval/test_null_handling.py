"""
Tests for eval command - Null handling functions.

Functions tested: coalesce, isnull, isnotnull, nullif
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestCoalesce:
    """Tests for coalesce() function."""

    def test_coalesce_two_fields(self):
        """Coalesce two fields - return first non-null."""
        df = pd.DataFrame({
            "primary": ["A", None, "C", None],
            "fallback": ["X", "Y", "Z", "W"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval result=coalesce(primary, fallback)'
        result = CommandExecutor(cmd).execute()

        assert result["result"].tolist() == ["A", "Y", "C", "W"]

    def test_coalesce_all_null(self):
        """Coalesce with all null values."""
        df = pd.DataFrame({
            "a": [None, None],
            "b": [None, "value"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval result=coalesce(a, b)'
        result = CommandExecutor(cmd).execute()

        assert pd.isna(result["result"].iloc[0])
        assert result["result"].iloc[1] == "value"


class TestIsnull:
    """Tests for isnull() function."""

    def test_isnull_basic(self):
        """Check for null values."""
        df = pd.DataFrame({"value": [1, None, 3, None, 5]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval is_missing=isnull(value)'
        result = CommandExecutor(cmd).execute()

        assert result["is_missing"].tolist() == [False, True, False, True, False]

    def test_isnull_no_nulls(self):
        """Check when no null values exist."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval is_missing=isnull(value)'
        result = CommandExecutor(cmd).execute()

        assert all(result["is_missing"] == False)


class TestIsnotnull:
    """Tests for isnotnull() function."""

    def test_isnotnull_basic(self):
        """Check for non-null values."""
        df = pd.DataFrame({"value": [1, None, 3, None, 5]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval has_value=isnotnull(value)'
        result = CommandExecutor(cmd).execute()

        assert result["has_value"].tolist() == [True, False, True, False, True]


class TestNullif:
    """Tests for nullif() function."""

    def test_nullif_basic(self):
        """Replace value with null if matches."""
        df = pd.DataFrame({"value": [1, 0, 3, 0, 5]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval cleaned=nullif(value, 0)'
        result = CommandExecutor(cmd).execute()

        assert result["cleaned"].iloc[0] == 1
        assert pd.isna(result["cleaned"].iloc[1])
        assert result["cleaned"].iloc[2] == 3
        assert pd.isna(result["cleaned"].iloc[3])
        assert result["cleaned"].iloc[4] == 5

    def test_nullif_no_match(self):
        """Nullif with no matching values."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval cleaned=nullif(value, 0)'
        result = CommandExecutor(cmd).execute()

        assert result["cleaned"].tolist() == [1, 2, 3]


class TestCombinedNullHandling:
    """Tests for combined null handling."""

    def test_isnull_isnotnull_complement(self):
        """Isnull and isnotnull should be complements."""
        df = pd.DataFrame({"value": [1, None, 3]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval is_null=isnull(value) | eval is_not_null=isnotnull(value)'
        result = CommandExecutor(cmd).execute()

        # They should be complementary
        for i in range(len(result)):
            assert result["is_null"].iloc[i] != result["is_not_null"].iloc[i]

    def test_coalesce_for_log_normalization(self):
        """Practical: normalize log levels using coalesce."""
        df = pd.DataFrame({
            "level": ["INFO", None, "ERROR", None],
            "severity": [None, "LOW", None, "HIGH"],
        })
        register_cache("logs", df)

        cmd = 'cache=logs | eval log_level=coalesce(level, severity)'
        result = CommandExecutor(cmd).execute()

        assert result["log_level"].tolist() == ["INFO", "LOW", "ERROR", "HIGH"]

