#!/usr/bin/env python
"""
Minimal test for the data_cmd refactored system.

This script tests the basic functionality of the new architecture
with a complex multi-join query similar to the user's requirement.

Run from data_cmd directory:
    uv run python test_minimal.py

Or from parent directory:
    python -m data_cmd.test_minimal
"""

from executors import CommandExecutor, register_cache, clear_cache

import pandas as pd

print("=" * 80)
print("Data CMD - Minimal Test")
print("=" * 80)
print()

# Clear any existing cache
clear_cache()

# ============================================================================
# Create test data
# ============================================================================
print("1. Creating test data...")

# Orders data (main data)
orders_df = pd.DataFrame({
    "order_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "customer_id": ["C001", "C002", "C001", "C003", "C002", "C001", "C003", "C002", "C003", "C001"],
    "product_id": ["P001", "P002", "P001", "P003", "P002", "P003", "P001", "P003", "P002", "P001"],
    "amount": [100.0, 250.0, 150.0, 300.0, 200.0, 175.0, 225.0, 350.0, 275.0, 125.0],
})
register_cache("test_data", orders_df)
print(f"  - orders (test_data): {len(orders_df)} rows")
print(orders_df)
print()

# Customers data
customers_df = pd.DataFrame({
    "customer_id": ["C001", "C002", "C003"],
    "segment": ["Premium", "Standard", "Premium"],
    "region": ["North", "South", "East"],
})
register_cache("customers", customers_df)
print(f"  - customers: {len(customers_df)} rows")
print(customers_df)
print()

# Products data
products_df = pd.DataFrame({
    "product_id": ["P001", "P002", "P003"],
    "category": ["Electronics", "Clothing", "Electronics"],
})
register_cache("products", products_df)
print(f"  - products: {len(products_df)} rows")
print(products_df)
print()

# ============================================================================
# Test 1: Simple cache source
# ============================================================================
print("=" * 80)
print("Test 1: Simple cache source")
print("=" * 80)
cmd = "cache=test_data"
print(f"Command: {cmd}")
result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result.head())
print()

# ============================================================================
# Test 2: Stats with by clause
# ============================================================================
print("=" * 80)
print("Test 2: Stats with groupby")
print("=" * 80)
cmd = "cache=test_data | stats sum(amount) as total, count as order_count by customer_id"
print(f"Command: {cmd}")
result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result)
print()

# ============================================================================
# Test 3: Sort descending
# ============================================================================
print("=" * 80)
print("Test 3: Sort descending")
print("=" * 80)
cmd = "cache=test_data | stats sum(amount) as total by customer_id | sort -total"
print(f"Command: {cmd}")
result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result)
print()

# ============================================================================
# Test 4: Simple subquery (search index)
# ============================================================================
print("=" * 80)
print("Test 4: Search subquery")
print("=" * 80)
cmd = 'cache=test_data | join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id]'
print(f"Command: {cmd}")
result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result)
print()

# ============================================================================
# Test 5: Full complex query
# ============================================================================
print("=" * 80)
print("Test 5: Full complex query with multiple joins")
print("=" * 80)
cmd = (
    'cache=test_data '
    '| join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id] '
    '| join product_id [search index="products" | stats first(category) as category by product_id] '
    '| stats sum(amount) as total_sales, count as order_count, avg(amount) as avg_order_value by segment, region, category '
    '| sort -total_sales'
)
print(f"Command: {cmd}")
print()
result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result)
print()

# ============================================================================
# Test 6: Head command
# ============================================================================
print("=" * 80)
print("Test 6: Head command")
print("=" * 80)
cmd = "cache=test_data | head 5"
print(f"Command: {cmd}")
result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result)
print()

# ============================================================================
# Test 7: Filter command
# ============================================================================
print("=" * 80)
print("Test 7: Filter command")
print("=" * 80)
cmd = 'cache=test_data | filter customer_id="C001"'
print(f"Command: {cmd}")
result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result)
print()

print("=" * 80)
print("All tests completed!")
print("=" * 80)

