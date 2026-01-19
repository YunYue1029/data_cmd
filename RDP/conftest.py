"""
Pytest configuration and shared fixtures.

This module provides:
- Automatic cache cleanup before/after each test
- Common test data fixtures for basic and advanced tests
- Helper fixtures for registering data to cache
"""

import numpy as np
import pandas as pd
import pytest

from RDP.executors import clear_cache, register_cache


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Clear cache before and after each test."""
    clear_cache()
    yield
    clear_cache()


@pytest.fixture(autouse=True)
def set_random_seed():
    """Set random seed for reproducibility."""
    np.random.seed(42)


@pytest.fixture
def customers_df() -> pd.DataFrame:
    """Sample customers data with segments and regions."""
    return pd.DataFrame({
        "customer_id": [f"C{i:03d}" for i in range(1, 11)],
        "segment": [
            "Premium", "Standard", "Basic", "Premium", "Standard",
            "Basic", "Premium", "Standard", "Basic", "Premium"
        ],
        "region": [
            "North", "North", "North", "South", "South",
            "South", "East", "East", "West", "West"
        ],
    })


@pytest.fixture
def orders_df() -> pd.DataFrame:
    """Sample orders data with random amounts."""
    np.random.seed(42)
    n_orders = 50
    return pd.DataFrame({
        "order_id": range(1, n_orders + 1),
        "customer_id": np.random.choice(
            [f"C{i:03d}" for i in range(1, 11)], n_orders
        ),
        "product_id": np.random.choice(
            ["P001", "P002", "P003", "P004", "P005"], n_orders
        ),
        "amount": np.round(np.random.uniform(50, 500, n_orders), 2),
    })


@pytest.fixture
def simple_df() -> pd.DataFrame:
    """Simple test data for basic command tests."""
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "department": ["Sales", "IT", "IT", "Sales", "HR"],
        "salary": [50000, 60000, 55000, 52000, 48000],
        "age": [30, 25, 35, 28, 32],
    })


@pytest.fixture
def logs_df() -> pd.DataFrame:
    """Sample log data for testing."""
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=100, freq="h"),
        "level": np.random.choice(["INFO", "WARN", "ERROR"], 100),
        "endpoint": np.random.choice(["/api/users", "/api/orders", "/api/products"], 100),
        "duration": np.random.uniform(10, 500, 100).round(2),
        "status_code": np.random.choice([200, 201, 400, 404, 500], 100),
    })


@pytest.fixture
def register_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Register customers to cache and return it."""
    register_cache("customers", customers_df)
    return customers_df


@pytest.fixture
def register_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Register orders to cache and return it."""
    register_cache("orders", orders_df)
    return orders_df


@pytest.fixture
def register_simple(simple_df: pd.DataFrame) -> pd.DataFrame:
    """Register simple data to cache and return it."""
    register_cache("data", simple_df)
    return simple_df


@pytest.fixture
def register_logs(logs_df: pd.DataFrame) -> pd.DataFrame:
    """Register logs to cache and return it."""
    register_cache("logs", logs_df)
    return logs_df

