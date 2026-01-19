"""
Tests for eval command - String functions.

Functions tested: upper, lower, len, substr, replace, trim
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestUpper:
    """Tests for upper() function."""

    def test_upper_basic(self):
        """Convert to uppercase."""
        df = pd.DataFrame({"name": ["john", "Jane", "BOB"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval upper_name=upper(name)'
        result = CommandExecutor(cmd).execute()

        assert result["upper_name"].tolist() == ["JOHN", "JANE", "BOB"]


class TestLower:
    """Tests for lower() function."""

    def test_lower_basic(self):
        """Convert to lowercase."""
        df = pd.DataFrame({"name": ["JOHN", "Jane", "bob"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval lower_name=lower(name)'
        result = CommandExecutor(cmd).execute()

        assert result["lower_name"].tolist() == ["john", "jane", "bob"]


class TestLen:
    """Tests for len() function."""

    def test_len_basic(self):
        """Get string length."""
        df = pd.DataFrame({"text": ["a", "abc", "abcdef"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval length=len(text)'
        result = CommandExecutor(cmd).execute()

        assert result["length"].tolist() == [1, 3, 6]

    def test_len_with_spaces(self):
        """Get length including spaces."""
        df = pd.DataFrame({"text": ["hello world", "a b c"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval length=len(text)'
        result = CommandExecutor(cmd).execute()

        assert result["length"].tolist() == [11, 5]


class TestSubstr:
    """Tests for substr() function."""

    def test_substr_from_start(self):
        """Substring from start position."""
        df = pd.DataFrame({"text": ["abcdefgh", "12345678"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval first_three=substr(text, 0, 3)'
        result = CommandExecutor(cmd).execute()

        assert result["first_three"].tolist() == ["abc", "123"]

    def test_substr_middle(self):
        """Substring from middle position."""
        df = pd.DataFrame({"text": ["abcdefgh"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval middle=substr(text, 2, 4)'
        result = CommandExecutor(cmd).execute()

        assert result["middle"].iloc[0] == "cdef"


class TestReplace:
    """Tests for replace() function."""

    def test_replace_word(self):
        """Replace word in string."""
        df = pd.DataFrame({"text": ["hello world", "hello universe"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval new_text=replace(text, "hello", "hi")'
        result = CommandExecutor(cmd).execute()

        assert result["new_text"].tolist() == ["hi world", "hi universe"]

    def test_replace_no_match(self):
        """Replace with no match returns original."""
        df = pd.DataFrame({"text": ["hello world"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval new_text=replace(text, "xyz", "abc")'
        result = CommandExecutor(cmd).execute()

        assert result["new_text"].iloc[0] == "hello world"


class TestTrim:
    """Tests for trim() function."""

    def test_trim_basic(self):
        """Trim whitespace from both ends."""
        df = pd.DataFrame({"text": ["  hello  ", "world  ", "  test"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval trimmed=trim(text)'
        result = CommandExecutor(cmd).execute()

        assert result["trimmed"].tolist() == ["hello", "world", "test"]


class TestCombinedStringFunctions:
    """Tests for combined string functions."""

    def test_upper_and_len(self):
        """Combine upper and len."""
        df = pd.DataFrame({"name": ["john", "jane"]})
        register_cache("test_data", df)

        cmd = 'cache=test_data | eval upper_name=upper(name) | eval name_len=len(name)'
        result = CommandExecutor(cmd).execute()

        assert result["upper_name"].tolist() == ["JOHN", "JANE"]
        assert result["name_len"].tolist() == [4, 4]

