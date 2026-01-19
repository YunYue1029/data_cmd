"""
Tests for eval command - Complex expressions and calculations.

Covers:
- Basic arithmetic operations
- Conditional expressions (if, case)
- String functions
- Time/Date functions (strftime, strptime)
- Coalesce and null handling
- Chained eval operations
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from RDP.executors import CommandExecutor, register_cache


class TestBasicArithmetic:
    """Tests for basic arithmetic operations in eval."""

    def test_simple_subtraction(self, sample_financial_data):
        """Test basic subtraction: profit = revenue - cost"""
        cmd = 'cache=financial | eval profit=revenue-cost'
        result = CommandExecutor(cmd).execute()
        
        assert "profit" in result.columns
        expected = sample_financial_data["revenue"] - sample_financial_data["cost"]
        pd.testing.assert_series_equal(result["profit"], expected, check_names=False)

    def test_multiplication(self, sample_orders):
        """Test multiplication: total = amount * quantity"""
        cmd = 'cache=orders | eval total=amount*quantity'
        result = CommandExecutor(cmd).execute()
        
        assert "total" in result.columns
        expected = sample_orders["amount"] * sample_orders["quantity"]
        pd.testing.assert_series_equal(result["total"], expected, check_names=False)

    def test_division(self, sample_financial_data):
        """Test division with parentheses: margin = (revenue - cost) / revenue"""
        cmd = 'cache=financial | eval margin=(revenue-cost)/revenue'
        result = CommandExecutor(cmd).execute()
        
        assert "margin" in result.columns
        expected = (sample_financial_data["revenue"] - sample_financial_data["cost"]) / sample_financial_data["revenue"]
        pd.testing.assert_series_equal(result["margin"], expected, check_names=False, atol=0.0001)

    def test_multiplication_with_constant(self, sample_financial_data):
        """Test multiplication with constant: percentage = margin * 100"""
        cmd = 'cache=financial | eval profit=revenue-cost | eval margin=(profit/revenue)*100'
        result = CommandExecutor(cmd).execute()
        
        assert "margin" in result.columns
        expected = ((sample_financial_data["revenue"] - sample_financial_data["cost"]) / 
                   sample_financial_data["revenue"]) * 100
        pd.testing.assert_series_equal(result["margin"], expected, check_names=False, atol=0.0001)


class TestConditionalExpressions:
    """Tests for conditional expressions (if, case)."""

    def test_if_simple(self, sample_financial_data):
        """
        Test simple if expression.
        
        eval profit_margin=if(revenue>0, (profit/revenue)*100, 0)
        """
        cmd = 'cache=financial | eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0)'
        result = CommandExecutor(cmd).execute()
        
        assert "profit_margin" in result.columns
        # All revenue > 0, so should have calculated margins
        assert not result["profit_margin"].isna().any()

    def test_if_with_zero_handling(self):
        """Test if expression handling zero division."""
        df = pd.DataFrame({
            "revenue": [100, 0, 200, 0, 150],
            "cost": [50, 25, 100, 10, 75],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0)'
        result = CommandExecutor(cmd).execute()
        
        # Rows with revenue=0 should have profit_margin=0
        assert result[result["revenue"] == 0]["profit_margin"].tolist() == [0, 0]

    def test_case_simple(self, sample_financial_data):
        """
        Test simple case expression.
        
        eval category=case(profit_margin>20, "High", profit_margin>10, "Medium", profit_margin>0, "Low", 1=1, "Loss")
        """
        cmd = '''cache=financial | eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0) | eval category=case(profit_margin>20, "High", profit_margin>10, "Medium", profit_margin>0, "Low", 1=1, "Loss")'''
        result = CommandExecutor(cmd).execute()
        
        assert "category" in result.columns
        # All categories should be one of the defined values
        valid_categories = {"High", "Medium", "Low", "Loss"}
        assert set(result["category"].unique()).issubset(valid_categories)

    def test_case_with_multiple_conditions(self):
        """Test case with multiple conditions."""
        df = pd.DataFrame({
            "score": [95, 85, 75, 65, 55, 45],
        })
        register_cache("scores", df)
        
        cmd = '''cache=scores | eval grade=case(score>=90, "A", score>=80, "B", score>=70, "C", score>=60, "D", 1=1, "F")'''
        result = CommandExecutor(cmd).execute()
        
        assert "grade" in result.columns
        expected_grades = ["A", "B", "C", "D", "F", "F"]
        assert result["grade"].tolist() == expected_grades

    def test_nested_if(self):
        """Test nested if expressions."""
        df = pd.DataFrame({
            "status": ["active", "inactive", "pending"],
            "count": [10, 5, 3],
        })
        register_cache("test_data", df)
        
        cmd = '''cache=test_data | eval priority=if(status="active", if(count>5, "high", "medium"), "low")'''
        result = CommandExecutor(cmd).execute()
        
        assert "priority" in result.columns
        assert result[result["status"] == "active"]["priority"].iloc[0] == "high"


class TestComplexEvalExpression:
    """Tests for complex eval expressions from user examples."""

    def test_profit_calculation_chain(self, sample_financial_data):
        """
        Test chained eval for profit calculation.
        
        eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0) | 
        eval category=case(profit_margin>20, "High", profit_margin>10, "Medium", profit_margin>0, "Low", 1=1, "Loss")
        """
        cmd = '''cache=financial | eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0) | eval category=case(profit_margin>20, "High", profit_margin>10, "Medium", profit_margin>0, "Low", 1=1, "Loss")'''
        result = CommandExecutor(cmd).execute()
        
        # Verify all calculated fields exist
        assert "profit" in result.columns
        assert "profit_margin" in result.columns
        assert "category" in result.columns
        
        # Verify category logic
        for _, row in result.iterrows():
            margin = row["profit_margin"]
            category = row["category"]
            
            if margin > 20:
                assert category == "High"
            elif margin > 10:
                assert category == "Medium"
            elif margin > 0:
                assert category == "Low"
            else:
                assert category == "Loss"


class TestStringFunctions:
    """Tests for string manipulation functions."""

    def test_upper_lower(self):
        """Test upper() and lower() functions."""
        df = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval upper_name=upper(name) | eval lower_name=lower(name)'
        result = CommandExecutor(cmd).execute()
        
        assert result["upper_name"].tolist() == ["JOHN", "JANE", "BOB"]
        assert result["lower_name"].tolist() == ["john", "jane", "bob"]

    def test_len_function(self):
        """Test len() function."""
        df = pd.DataFrame({
            "text": ["short", "medium length", "this is a longer text"],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval text_length=len(text)'
        result = CommandExecutor(cmd).execute()
        
        assert result["text_length"].tolist() == [5, 13, 21]

    def test_substr_function(self):
        """Test substr() function."""
        df = pd.DataFrame({
            "text": ["abcdefgh", "12345678", "xyz12345"],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval first_three=substr(text, 0, 3)'
        result = CommandExecutor(cmd).execute()
        
        assert result["first_three"].tolist() == ["abc", "123", "xyz"]

    def test_replace_function(self):
        """Test replace() function."""
        df = pd.DataFrame({
            "text": ["hello world", "hello universe", "goodbye world"],
        })
        register_cache("test_data", df)
        
        cmd = '''cache=test_data | eval new_text=replace(text, "world", "planet")'''
        result = CommandExecutor(cmd).execute()
        
        assert result["new_text"].tolist() == ["hello planet", "hello universe", "goodbye planet"]


class TestTimeFunctions:
    """Tests for time and date functions."""

    def test_strftime_hour_extraction(self, sample_server_metrics):
        """
        Test strftime for hour extraction.
        
        eval hour=strftime(_time, "%H")
        """
        cmd = '''cache=server_metrics | eval hour=strftime(_time, "%H")'''
        result = CommandExecutor(cmd).execute()
        
        assert "hour" in result.columns
        # Hours should be 2-digit strings
        assert all(len(str(h)) <= 2 for h in result["hour"].unique())

    def test_strftime_date_format(self, sample_orders):
        """Test strftime with date format."""
        cmd = '''cache=orders | eval date_str=strftime(order_date, "%Y-%m-%d")'''
        result = CommandExecutor(cmd).execute()
        
        assert "date_str" in result.columns
        # Should be in YYYY-MM-DD format
        assert all("-" in str(d) for d in result["date_str"])

    def test_strptime_parsing(self):
        """
        Test strptime for parsing timestamp strings.
        
        eval parsed_time=strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        """
        df = pd.DataFrame({
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-02 15:30:00",
                "2024-01-03 20:45:00",
            ],
        })
        register_cache("test_data", df)
        
        cmd = '''cache=test_data | eval parsed_time=strptime(timestamp, "%Y-%m-%d %H:%M:%S")'''
        result = CommandExecutor(cmd).execute()
        
        assert "parsed_time" in result.columns
        # Should be datetime type
        assert pd.api.types.is_datetime64_any_dtype(result["parsed_time"])

    def test_time_extraction_functions(self, sample_orders):
        """Test year(), month(), day() functions."""
        cmd = 'cache=orders | eval year=year(order_date) | eval month=month(order_date) | eval day=day(order_date)'
        result = CommandExecutor(cmd).execute()
        
        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns
        
        # Year should be 2024 for our test data
        assert 2024 in result["year"].unique()


class TestTimeSeriesAnalysis:
    """Tests for time series analysis with eval."""

    def test_hourly_stats_with_eval(self, sample_server_metrics):
        """
        Test time series analysis: extract hour and aggregate.
        
        eval hour=strftime(_time, "%H") | stats avg(cpu_usage) as avg_cpu, max(memory_usage) as max_mem by host, hour
        """
        cmd = '''cache=server_metrics | eval hour=strftime(_time, "%H") | stats avg(cpu_usage) as avg_cpu, max(memory_usage) as max_mem by host, hour'''
        result = CommandExecutor(cmd).execute()
        
        assert "host" in result.columns
        assert "hour" in result.columns
        assert "avg_cpu" in result.columns
        assert "max_mem" in result.columns
        
        # Should have multiple hours
        assert len(result["hour"].unique()) > 1


class TestCoalesceAndNullHandling:
    """Tests for coalesce and null handling functions."""

    def test_coalesce_simple(self):
        """Test coalesce() function."""
        df = pd.DataFrame({
            "level": ["INFO", None, "ERROR", None, "WARN"],
            "severity": [None, "LOW", None, "HIGH", None],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval log_level=coalesce(level, severity)'
        result = CommandExecutor(cmd).execute()
        
        assert "log_level" in result.columns
        # Should use level if not null, otherwise severity
        expected = ["INFO", "LOW", "ERROR", "HIGH", "WARN"]
        assert result["log_level"].tolist() == expected

    def test_isnull_isnotnull(self):
        """Test isnull() and isnotnull() functions."""
        df = pd.DataFrame({
            "value": [1, None, 3, None, 5],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval is_missing=isnull(value) | eval has_value=isnotnull(value)'
        result = CommandExecutor(cmd).execute()
        
        assert result["is_missing"].tolist() == [False, True, False, True, False]
        assert result["has_value"].tolist() == [True, False, True, False, True]

    def test_nullif_function(self):
        """Test nullif() function."""
        df = pd.DataFrame({
            "value": [1, 0, 3, 0, 5],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval cleaned=nullif(value, 0)'
        result = CommandExecutor(cmd).execute()
        
        assert pd.isna(result["cleaned"].iloc[1])
        assert pd.isna(result["cleaned"].iloc[3])
        assert result["cleaned"].iloc[0] == 1


class TestMathFunctions:
    """Tests for mathematical functions."""

    def test_abs_function(self):
        """Test abs() function."""
        df = pd.DataFrame({
            "value": [-5, 3, -10, 7, -2],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval abs_value=abs(value)'
        result = CommandExecutor(cmd).execute()
        
        assert result["abs_value"].tolist() == [5, 3, 10, 7, 2]

    def test_round_function(self):
        """Test round() function."""
        df = pd.DataFrame({
            "value": [3.14159, 2.71828, 1.41421, 9.99999],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval rounded=round(value, 2)'
        result = CommandExecutor(cmd).execute()
        
        expected = [3.14, 2.72, 1.41, 10.0]
        assert result["rounded"].tolist() == expected

    def test_sqrt_function(self):
        """Test sqrt() function."""
        df = pd.DataFrame({
            "value": [4, 9, 16, 25],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval root=sqrt(value)'
        result = CommandExecutor(cmd).execute()
        
        assert result["root"].tolist() == [2.0, 3.0, 4.0, 5.0]

    def test_ceil_floor_functions(self):
        """Test ceil() and floor() functions."""
        df = pd.DataFrame({
            "value": [3.2, 3.8, -3.2, -3.8],
        })
        register_cache("test_data", df)
        
        cmd = 'cache=test_data | eval ceiling=ceil(value) | eval floored=floor(value)'
        result = CommandExecutor(cmd).execute()
        
        assert result["ceiling"].tolist() == [4.0, 4.0, -3.0, -3.0]
        assert result["floored"].tolist() == [3.0, 3.0, -4.0, -4.0]


class TestComplexEvalPipelines:
    """Integration tests for complex eval pipelines."""

    def test_full_financial_analysis(self, sample_financial_data):
        """
        Complete financial analysis pipeline.
        
        eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0) |
        eval category=case(...) | stats avg(profit_margin) as avg_margin by category
        """
        cmd = '''cache=financial | eval profit=revenue-cost | eval profit_margin=if(revenue>0, (profit/revenue)*100, 0) | eval category=case(profit_margin>20, "High", profit_margin>10, "Medium", profit_margin>0, "Low", 1=1, "Loss") | stats avg(profit_margin) as avg_margin, count as n by category'''
        result = CommandExecutor(cmd).execute()
        
        assert "category" in result.columns
        assert "avg_margin" in result.columns
        assert "n" in result.columns
        
        # Total count should match original
        assert result["n"].sum() == len(sample_financial_data)

    def test_eval_with_multiple_conditions_and_stats(self, sample_web_logs):
        """
        Complex pipeline combining eval conditions with stats.
        
        eval response_category=case(response_time<100, "fast", response_time<500, "normal", 1=1, "slow") |
        stats count, avg(response_time) by response_category, status_code
        """
        cmd = '''cache=web_logs | eval response_category=case(response_time<100, "fast", response_time<500, "normal", 1=1, "slow") | stats count as n, avg(response_time) as avg_time by response_category, status_code'''
        result = CommandExecutor(cmd).execute()
        
        assert "response_category" in result.columns
        assert "status_code" in result.columns
        assert "n" in result.columns
        assert "avg_time" in result.columns
        
        # Verify categorization
        valid_categories = {"fast", "normal", "slow"}
        assert set(result["response_category"].unique()).issubset(valid_categories)

