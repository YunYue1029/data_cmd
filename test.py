import pandas as pd
import numpy as np
from executors import CommandExecutor, register_cache, clear_cache

# Set random seed for reproducibility
np.random.seed(42)

clear_cache()

# ============================================================================
# Create larger test data
# ============================================================================

# 10 customers with different segments and regions
customers_df = pd.DataFrame({
    "customer_id": [f"C{i:03d}" for i in range(1, 11)],
    "segment": ["Premium", "Standard", "Basic", "Premium", "Standard", 
                "Basic", "Premium", "Standard", "Basic", "Premium"],
    "region": ["North", "North", "North", "South", "South", 
               "South", "East", "East", "West", "West"],
})
register_cache("customers", customers_df)

print("=" * 60)
print("Customers (10 rows):")
print("=" * 60)
print(customers_df)
print()

# 50 orders with random customers and amounts
n_orders = 50
orders_df = pd.DataFrame({
    "order_id": range(1, n_orders + 1),
    "customer_id": np.random.choice([f"C{i:03d}" for i in range(1, 11)], n_orders),
    "product_id": np.random.choice(["P001", "P002", "P003", "P004", "P005"], n_orders),
    "amount": np.round(np.random.uniform(50, 500, n_orders), 2),
})
register_cache("test_data", orders_df)

print("=" * 60)
print(f"Orders (test_data) - {n_orders} rows:")
print("=" * 60)
print(orders_df)
print()

# ============================================================================
# Manual verification
# ============================================================================
print("=" * 60)
print("Manual Verification Steps")
print("=" * 60)
print()

# Step 1: Subquery result
print("Step 1: Subquery - stats first(segment), first(region) by customer_id")
print("-" * 60)
subquery_result = customers_df.groupby("customer_id", as_index=False).agg(
    segment=("segment", "first"),
    region=("region", "first")
)
print(subquery_result)
print()

# Step 2: Join result
print("Step 2: Join orders with subquery result on customer_id")
print("-" * 60)
manual_result = orders_df.merge(subquery_result, on="customer_id", how="left")
print(manual_result)
print()

# ============================================================================
# Execute via CommandExecutor
# ============================================================================
print("=" * 60)
print("CommandExecutor Result")
print("=" * 60)

cmd = 'cache=test_data | join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id]'
print(f"Command: {cmd}")
print()

result = CommandExecutor(cmd).execute()
print(f"Result: {len(result)} rows")
print(result)
print()

# ============================================================================
# Compare results
# ============================================================================
print("=" * 60)
print("Verification: Are results equal?")
print("=" * 60)

# Sort both for comparison
manual_sorted = manual_result.sort_values("order_id").reset_index(drop=True)
result_sorted = result.sort_values("order_id").reset_index(drop=True)

# Compare
try:
    pd.testing.assert_frame_equal(manual_sorted, result_sorted)
    print("SUCCESS: Manual result matches CommandExecutor result!")
except AssertionError as e:
    print(f"MISMATCH: {e}")

print()

# ============================================================================
# Additional stats for understanding
# ============================================================================
print("=" * 60)
print("Summary Statistics by Segment and Region")
print("=" * 60)

summary = result.groupby(["segment", "region"]).agg(
    order_count=("order_id", "count"),
    total_amount=("amount", "sum"),
    avg_amount=("amount", "mean")
).round(2)
print(summary)