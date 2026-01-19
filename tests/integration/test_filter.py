"""
Tests for filter/where command - Filtering and conditional logic.

Covers:
- Basic comparison operators (=, !=, >, <, >=, <=)
- Boolean operators (AND, OR, NOT)
- Complex conditional filtering
- Filter with regex
- Chained filters
"""

import pytest
import pandas as pd
import numpy as np

from RDP.executors import CommandExecutor, register_cache


class TestBasicComparisons:
    """Tests for basic comparison operators."""

    def test_equals_string(self, sample_web_logs):
        """Test equality filter with string."""
        cmd = 'cache=web_logs | filter host="web01"'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["host"] == "web01")
        expected_count = len(sample_web_logs[sample_web_logs["host"] == "web01"])
        assert len(result) == expected_count

    def test_equals_number(self, sample_web_logs):
        """Test equality filter with number."""
        cmd = 'cache=web_logs | filter status_code=200'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"] == 200)
        expected_count = len(sample_web_logs[sample_web_logs["status_code"] == 200])
        assert len(result) == expected_count

    def test_not_equals(self, sample_web_logs):
        """Test not equals filter."""
        cmd = 'cache=web_logs | filter status_code!=200'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"] != 200)
        expected_count = len(sample_web_logs[sample_web_logs["status_code"] != 200])
        assert len(result) == expected_count

    def test_greater_than(self, sample_web_logs):
        """Test greater than filter."""
        cmd = 'cache=web_logs | filter response_time>100'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["response_time"] > 100)
        expected_count = len(sample_web_logs[sample_web_logs["response_time"] > 100])
        assert len(result) == expected_count

    def test_less_than(self, sample_web_logs):
        """Test less than filter."""
        cmd = 'cache=web_logs | filter response_time<50'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["response_time"] < 50)
        expected_count = len(sample_web_logs[sample_web_logs["response_time"] < 50])
        assert len(result) == expected_count

    def test_greater_than_equals(self, sample_web_logs):
        """Test greater than or equals filter."""
        cmd = 'cache=web_logs | filter status_code>=400'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"] >= 400)
        expected_count = len(sample_web_logs[sample_web_logs["status_code"] >= 400])
        assert len(result) == expected_count

    def test_less_than_equals(self, sample_web_logs):
        """Test less than or equals filter."""
        cmd = 'cache=web_logs | filter status_code<=201'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"] <= 201)
        expected_count = len(sample_web_logs[sample_web_logs["status_code"] <= 201])
        assert len(result) == expected_count


class TestWhereCommand:
    """Tests for where command (alias for filter with expressions)."""

    def test_where_simple_condition(self, sample_web_logs):
        """Test where with simple condition."""
        cmd = 'cache=web_logs | where status_code >= 400'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"] >= 400)

    def test_where_compound_condition_and(self, sample_web_logs):
        """
        Test where with AND condition.
        
        where status_code >= 400 AND status_code < 500
        """
        cmd = 'cache=web_logs | where status_code >= 400 AND status_code < 500'
        result = CommandExecutor(cmd).execute()
        
        assert all((result["status_code"] >= 400) & (result["status_code"] < 500))
        
        # Verify count
        expected = sample_web_logs[
            (sample_web_logs["status_code"] >= 400) & 
            (sample_web_logs["status_code"] < 500)
        ]
        assert len(result) == len(expected)

    def test_where_compound_condition_or(self, sample_web_logs):
        """
        Test where with OR condition.
        
        where status_code = 200 OR status_code = 201
        """
        cmd = 'cache=web_logs | where status_code = 200 OR status_code = 201'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"].isin([200, 201]))

    def test_where_with_not(self, sample_web_logs):
        """
        Test where with NOT condition.
        
        where NOT status_code = 200
        """
        cmd = 'cache=web_logs | where NOT status_code = 200'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"] != 200)

    def test_where_complex_boolean(self, sample_web_logs):
        """
        Test where with complex boolean expression.
        
        where (status_code >= 400 AND status_code < 500) OR response_time > 200
        """
        cmd = 'cache=web_logs | where (status_code >= 400 AND status_code < 500) OR response_time > 200'
        result = CommandExecutor(cmd).execute()
        
        # Verify condition
        for _, row in result.iterrows():
            is_4xx = 400 <= row["status_code"] < 500
            is_slow = row["response_time"] > 200
            assert is_4xx or is_slow


class TestComplexFiltering:
    """Tests for complex filtering scenarios."""

    def test_filter_with_rex_and_where(self, sample_web_logs):
        """
        Test complex filter combining rex and where.
        
        rex field=uri "(?<endpoint>/[^?]+)" | where status_code >= 400 AND status_code < 500 | 
        stats count as error_count, dc(ip) as unique_ips by endpoint, status_code | where error_count > 10
        """
        # This test combines multiple operations
        cmd = '''cache=web_logs | rex field=uri "(?<endpoint>/[^?]+)" | where status_code >= 400 AND status_code < 500 | stats count as error_count, dc(ip) as unique_ips by endpoint, status_code | where error_count > 1'''
        result = CommandExecutor(cmd).execute()
        
        # Should have endpoint, status_code, error_count, unique_ips columns
        if len(result) > 0:
            assert "endpoint" in result.columns
            assert "status_code" in result.columns
            assert "error_count" in result.columns
            assert "unique_ips" in result.columns
            
            # All error_count should be > 1
            assert all(result["error_count"] > 1)

    def test_chained_filters(self, sample_web_logs):
        """Test multiple chained filter commands."""
        cmd = 'cache=web_logs | filter host="web01" | filter status_code=200'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["host"] == "web01")
        assert all(result["status_code"] == 200)
        
        expected = sample_web_logs[
            (sample_web_logs["host"] == "web01") & 
            (sample_web_logs["status_code"] == 200)
        ]
        assert len(result) == len(expected)

    def test_filter_after_stats(self, sample_web_logs):
        """
        Test filter applied after stats aggregation.
        
        stats count by host | where count > 10
        """
        cmd = 'cache=web_logs | stats count as n by host | where n > 10'
        result = CommandExecutor(cmd).execute()
        
        # All counts should be > 10
        assert all(result["n"] > 10)


class TestFilterWithStringConditions:
    """Tests for filtering with string conditions."""

    def test_filter_string_contains(self, sample_web_logs):
        """
        Test filter with string contains (using where with like/match).
        
        where uri LIKE "%api%"
        """
        cmd = 'cache=web_logs | where uri LIKE "%api%"'
        result = CommandExecutor(cmd).execute()
        
        # All uri should contain "api"
        assert all("api" in uri for uri in result["uri"])

    def test_filter_string_startswith(self, sample_web_logs):
        """
        Test filter with string starts with.
        
        where uri LIKE "/api%"
        """
        cmd = 'cache=web_logs | where uri LIKE "/api%"'
        result = CommandExecutor(cmd).execute()
        
        # All uri should start with "/api"
        assert all(uri.startswith("/api") for uri in result["uri"])

    def test_filter_in_list(self, sample_web_logs):
        """
        Test filter with IN operator.
        
        where status_code IN (200, 201, 404)
        """
        cmd = 'cache=web_logs | where status_code IN (200, 201, 404)'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["status_code"].isin([200, 201, 404]))

    def test_filter_not_in_list(self, sample_web_logs):
        """
        Test filter with NOT IN operator.
        
        where status_code NOT IN (500, 404)
        """
        cmd = 'cache=web_logs | where status_code NOT IN (500, 404)'
        result = CommandExecutor(cmd).execute()
        
        assert all(~result["status_code"].isin([500, 404]))


class TestFilterWithNullValues:
    """Tests for filtering with null/missing values."""

    def test_filter_is_null(self):
        """Test filtering null values."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "value": [10, None, 30, None, 50],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | where isnull(value)'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 2
        assert all(pd.isna(result["value"]))

    def test_filter_is_not_null(self):
        """Test filtering non-null values."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "value": [10, None, 30, None, 50],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | where isnotnull(value)'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 3
        assert all(pd.notna(result["value"]))


class TestFilterEdgeCases:
    """Tests for filter edge cases."""

    def test_filter_empty_result(self, sample_web_logs):
        """Test filter that returns no results."""
        cmd = 'cache=web_logs | filter status_code=999'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == 0
        # Should still have correct columns
        assert list(result.columns) == list(sample_web_logs.columns)

    def test_filter_all_match(self, sample_web_logs):
        """Test filter where all rows match."""
        cmd = 'cache=web_logs | filter status_code>=0'
        result = CommandExecutor(cmd).execute()
        
        assert len(result) == len(sample_web_logs)

    def test_filter_with_calculated_field(self, sample_financial_data):
        """Test filter on a calculated field."""
        cmd = 'cache=financial | eval profit=revenue-cost | where profit > 0'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["profit"] > 0)


class TestFilterIntegration:
    """Integration tests for filter with other commands."""

    def test_filter_eval_stats_pipeline(self, sample_web_logs):
        """
        Test complete pipeline: filter -> eval -> stats.
        
        where status_code >= 400 | eval error_type=if(status_code<500, "client", "server") | stats count by error_type
        """
        cmd = '''cache=web_logs | where status_code >= 400 | eval error_type=if(status_code<500, "client", "server") | stats count as n by error_type'''
        result = CommandExecutor(cmd).execute()
        
        assert "error_type" in result.columns
        assert "n" in result.columns
        
        valid_types = {"client", "server"}
        assert set(result["error_type"].unique()).issubset(valid_types)

    def test_stats_filter_pipeline(self, sample_web_logs):
        """
        Test pipeline: stats -> filter.
        
        stats count, avg(response_time) as avg_time by host | where count > 5
        """
        cmd = 'cache=web_logs | stats count as n, avg(response_time) as avg_time by host | where n > 5'
        result = CommandExecutor(cmd).execute()
        
        assert all(result["n"] > 5)
        assert "avg_time" in result.columns

    def test_join_filter_pipeline(self, sample_orders, sample_customers):
        """
        Test pipeline: join -> filter -> stats.
        """
        cmd = '''cache=orders | join customer_id [search index="customers" | stats first(segment) as segment by customer_id] | where segment = "Premium" | stats sum(amount) as total by segment'''
        result = CommandExecutor(cmd).execute()
        
        # Should only have Premium segment
        assert len(result) == 1
        assert result["segment"].iloc[0] == "Premium"

