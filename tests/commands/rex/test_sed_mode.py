"""
Tests for rex command - Sed mode (replacement).

Pattern replacement using s/pattern/replacement/ syntax
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestSedBasic:
    """Tests for basic sed mode replacement."""

    def test_sed_simple_replacement(self):
        """Simple word replacement."""
        df = pd.DataFrame({
            "text": ["Hello World", "Hello Universe"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=text mode=sed "s/Hello/Hi/"'
        result = CommandExecutor(cmd).execute()

        assert result["text"].tolist() == ["Hi World", "Hi Universe"]

    def test_sed_number_replacement(self):
        """Replace numbers with placeholder."""
        df = pd.DataFrame({
            "text": ["order 123", "order 456"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=text mode=sed "s/\\d+/[NUM]/"'
        result = CommandExecutor(cmd).execute()

        assert result["text"].tolist() == ["order [NUM]", "order [NUM]"]


class TestSedPatternReplacement:
    """Tests for sed mode with pattern/replacement parameters."""

    def test_sed_with_explicit_params(self):
        """Sed using pattern and replacement parameters."""
        df = pd.DataFrame({
            "text": ["Hello World"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=text pattern="World" mode=sed replacement="Planet"'
        result = CommandExecutor(cmd).execute()

        assert result["text"].iloc[0] == "Hello Planet"

    def test_sed_regex_pattern(self):
        """Sed with regex pattern."""
        df = pd.DataFrame({
            "text": ["user123", "admin456"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=text pattern="\\d+" mode=sed replacement="[ID]"'
        result = CommandExecutor(cmd).execute()

        assert result["text"].tolist() == ["user[ID]", "admin[ID]"]


class TestSedNoMatch:
    """Tests for sed mode when pattern doesn't match."""

    def test_sed_no_match_unchanged(self):
        """Text unchanged when pattern doesn't match."""
        df = pd.DataFrame({
            "text": ["Hello World"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=text mode=sed "s/XYZ/ABC/"'
        result = CommandExecutor(cmd).execute()

        assert result["text"].iloc[0] == "Hello World"


class TestSedPractical:
    """Practical sed mode scenarios."""

    def test_mask_sensitive_data(self):
        """Mask sensitive data like emails."""
        df = pd.DataFrame({
            "log": ["User user@example.com logged in", "Contact admin@company.org"],
        })
        register_cache("test_data", df)

        cmd = 'cache=test_data | rex field=log pattern="[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}" mode=sed replacement="[EMAIL]"'
        result = CommandExecutor(cmd).execute()

        assert "[EMAIL]" in result["log"].iloc[0]
        assert "[EMAIL]" in result["log"].iloc[1]

