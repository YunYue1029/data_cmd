"""
Integration tests - Business analytics workflows.

Complex pipelines for business data analysis
"""

import pytest
import pandas as pd

from RDP.executors import CommandExecutor, register_cache


class TestOrderAnalysis:
    """Tests for order analysis workflows."""

    def test_revenue_by_category(self, sample_orders, sample_products):
        """Calculate revenue by product category."""
        cmd = '''cache=orders
        | join product_id [search index="products"]
        | stats sum(amount) as revenue by category'''
        result = CommandExecutor(cmd).execute()

        assert "category" in result.columns
        assert "revenue" in result.columns

    def test_avg_order_value(self, sample_orders, sample_customers):
        """Calculate average order value by customer segment."""
        cmd = '''cache=orders
        | join customer_id [search index="customers"]
        | stats avg(amount) as avg_order by segment'''
        result = CommandExecutor(cmd).execute()

        assert "segment" in result.columns
        assert "avg_order" in result.columns


class TestCustomerAnalysis:
    """Tests for customer analysis workflows."""

    def test_customer_order_count(self, sample_orders):
        """Count orders per customer."""
        cmd = '''cache=orders
        | stats count as order_count, sum(amount) as total_spent by customer_id'''
        result = CommandExecutor(cmd).execute()

        assert "customer_id" in result.columns
        assert "order_count" in result.columns
        assert "total_spent" in result.columns

    def test_customer_lifetime_value(self, sample_orders, sample_customers):
        """Calculate customer lifetime value metrics."""
        cmd = '''cache=orders
        | stats sum(amount) as total_revenue, count as orders, first(order_date) as first_order, last(order_date) as last_order by customer_id
        | join customer_id [search index="customers"]'''
        result = CommandExecutor(cmd).execute()

        assert "total_revenue" in result.columns
        assert "segment" in result.columns


class TestFinancialAnalysis:
    """Tests for financial analysis workflows."""

    def test_profit_margin(self, sample_financial_data):
        """Calculate profit margin."""
        cmd = '''cache=financial
        | eval profit=revenue-cost
        | eval margin=if(revenue>0, (profit/revenue)*100, 0)
        | stats avg(margin) as avg_margin, min(margin) as min_margin, max(margin) as max_margin by category'''
        result = CommandExecutor(cmd).execute()

        assert "avg_margin" in result.columns
        assert "min_margin" in result.columns
        assert "max_margin" in result.columns

    def test_profitability_categories(self, sample_financial_data):
        """Categorize products by profitability."""
        cmd = '''cache=financial
        | eval profit=revenue-cost
        | eval margin=if(revenue>0, (profit/revenue)*100, 0)
        | eval category=case(margin>30, "High", margin>15, "Medium", margin>0, "Low", 1=1, "Loss")
        | stats count by category'''
        result = CommandExecutor(cmd).execute()

        assert "category" in result.columns


class TestProductAnalysis:
    """Tests for product analysis workflows."""

    def test_product_performance(self, sample_orders, sample_products):
        """Analyze product performance."""
        cmd = '''cache=orders
        | join product_id [search index="products"]
        | stats sum(amount) as total_revenue, count as order_count by product_id
        | eval avg_order=total_revenue/order_count'''
        result = CommandExecutor(cmd).execute()

        assert "total_revenue" in result.columns
        assert "order_count" in result.columns
        assert "avg_order" in result.columns

