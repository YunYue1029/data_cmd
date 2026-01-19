"""
Pytest configuration and shared fixtures for data-cmd tests.
"""

import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta

from RDP.executors import CommandExecutor, register_cache, clear_cache


@pytest.fixture(autouse=True)
def setup_cache():
    """Clear cache before and after each test."""
    clear_cache()
    yield
    clear_cache()


@pytest.fixture
def sample_web_logs():
    """
    Sample web server logs for testing stats and filtering.
    Contains: timestamp, host, status_code, response_time, bytes, uri, method, ip
    """
    np.random.seed(42)
    n = 100
    
    hosts = ["web01", "web02", "web03"]
    endpoints = ["/api/users", "/api/orders", "/api/products", "/health", "/login"]
    methods = ["GET", "POST", "PUT", "DELETE"]
    status_codes = [200, 200, 200, 201, 400, 401, 404, 500]  # weighted toward 200
    
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    
    df = pd.DataFrame({
        "_time": [base_time + timedelta(seconds=i * 60) for i in range(n)],
        "host": np.random.choice(hosts, n),
        "status_code": np.random.choice(status_codes, n),
        "response_time": np.random.exponential(100, n).round(2),
        "bytes": np.random.randint(100, 10000, n),
        "uri": np.random.choice(endpoints, n),
        "method": np.random.choice(methods, n),
        "ip": [f"192.168.1.{np.random.randint(1, 255)}" for _ in range(n)],
    })
    
    register_cache("web_logs", df)
    return df


@pytest.fixture
def sample_user_info():
    """
    Sample user information for join tests.
    Contains: user_id, department, role, email
    """
    df = pd.DataFrame({
        "user_id": [f"U{i:03d}" for i in range(1, 21)],
        "department": ["Engineering", "Sales", "Marketing", "Engineering", "Sales",
                      "Marketing", "Engineering", "Sales", "Marketing", "Engineering",
                      "HR", "Finance", "HR", "Finance", "Engineering",
                      "Sales", "Marketing", "HR", "Finance", "Engineering"],
        "role": ["Developer", "Manager", "Analyst", "Developer", "Rep",
                "Analyst", "Lead", "Rep", "Manager", "Developer",
                "Specialist", "Analyst", "Manager", "Specialist", "Architect",
                "Director", "Lead", "Specialist", "Manager", "Developer"],
        "email": [f"user{i}@company.com" for i in range(1, 21)],
    })
    
    register_cache("user_info", df)
    return df


@pytest.fixture
def sample_customers():
    """
    Sample customer data for multi-join tests.
    Contains: customer_id, segment, region
    """
    df = pd.DataFrame({
        "customer_id": [f"C{i:03d}" for i in range(1, 31)],
        "segment": ["Premium", "Standard", "Basic"] * 10,
        "region": ["North", "South", "East", "West", "Central"] * 6,
    })
    
    register_cache("customers", df)
    return df


@pytest.fixture
def sample_products():
    """
    Sample product data for multi-join tests.
    Contains: product_id, category, price
    """
    df = pd.DataFrame({
        "product_id": [f"P{i:03d}" for i in range(1, 21)],
        "category": ["Electronics", "Clothing", "Food", "Electronics", "Clothing",
                    "Food", "Electronics", "Clothing", "Food", "Electronics",
                    "Books", "Sports", "Books", "Sports", "Electronics",
                    "Clothing", "Food", "Books", "Sports", "Electronics"],
        "price": [299.99, 49.99, 9.99, 599.99, 79.99,
                 14.99, 199.99, 29.99, 4.99, 899.99,
                 19.99, 89.99, 24.99, 149.99, 449.99,
                 59.99, 7.99, 14.99, 199.99, 349.99],
    })
    
    register_cache("products", df)
    return df


@pytest.fixture
def sample_orders():
    """
    Sample order data for aggregation and join tests.
    Contains: order_id, customer_id, product_id, amount, quantity, order_date
    """
    np.random.seed(42)
    n = 100
    
    base_date = datetime(2024, 1, 1)
    
    df = pd.DataFrame({
        "order_id": range(1, n + 1),
        "customer_id": [f"C{np.random.randint(1, 31):03d}" for _ in range(n)],
        "product_id": [f"P{np.random.randint(1, 21):03d}" for _ in range(n)],
        "amount": np.round(np.random.uniform(10, 500, n), 2),
        "quantity": np.random.randint(1, 10, n),
        "order_date": [base_date + timedelta(days=np.random.randint(0, 90)) for _ in range(n)],
    })
    
    register_cache("orders", df)
    return df


@pytest.fixture
def sample_financial_data():
    """
    Sample financial data for eval calculations.
    Contains: transaction_id, revenue, cost, category
    """
    np.random.seed(42)
    n = 50
    
    df = pd.DataFrame({
        "transaction_id": range(1, n + 1),
        "revenue": np.round(np.random.uniform(100, 1000, n), 2),
        "cost": np.round(np.random.uniform(50, 500, n), 2),
        "category": np.random.choice(["A", "B", "C", "D"], n),
    })
    
    register_cache("financial", df)
    return df


@pytest.fixture
def sample_server_metrics():
    """
    Sample server metrics for time series and anomaly detection.
    Contains: _time, host, cpu_usage, memory_usage, disk_io, response_time
    """
    np.random.seed(42)
    n = 200
    
    hosts = ["server01", "server02", "server03"]
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    
    data = []
    for i in range(n):
        for host in hosts:
            # Add some anomalies for server02
            cpu_base = 70 if host == "server02" and i > 150 else 30
            cpu = np.clip(np.random.normal(cpu_base, 10), 0, 100)
            
            memory_base = 80 if host == "server02" and i > 150 else 50
            memory = np.clip(np.random.normal(memory_base, 15), 0, 100)
            
            # Response time with occasional spikes
            response = np.random.exponential(100) if np.random.random() > 0.05 else np.random.exponential(500)
            
            data.append({
                "_time": base_time + timedelta(minutes=i * 5),
                "host": host,
                "cpu_usage": round(cpu, 2),
                "memory_usage": round(memory, 2),
                "disk_io": np.random.randint(100, 1000),
                "response_time": round(response, 2),
            })
    
    df = pd.DataFrame(data)
    register_cache("server_metrics", df)
    return df


@pytest.fixture
def sample_app_logs():
    """
    Sample application logs for rex and string parsing.
    Contains: _raw, timestamp, level, logger, message
    """
    logs = [
        "2024-01-01 10:00:00 INFO com.app.service - User login successful: user_id=U001",
        "2024-01-01 10:01:00 WARN com.app.auth - Failed login attempt from ip=192.168.1.100",
        "2024-01-01 10:02:00 ERROR com.app.db - Database connection failed: timeout=30s",
        "2024-01-01 10:03:00 INFO com.app.api - Request completed: endpoint=/api/users duration=150ms",
        "2024-01-01 10:04:00 DEBUG com.app.cache - Cache hit for key=user_session_123",
        "2024-01-01 10:05:00 ERROR com.app.service - NullPointerException at line 42",
        "2024-01-01 10:06:00 INFO com.app.api - Request completed: endpoint=/api/orders duration=200ms",
        "2024-01-01 10:07:00 WARN com.app.memory - Memory usage high: 85%",
        "2024-01-01 10:08:00 INFO com.app.service - Order created: order_id=12345 amount=299.99",
        "2024-01-01 10:09:00 ERROR com.app.network - Connection reset by peer",
    ]
    
    df = pd.DataFrame({
        "_raw": logs,
    })
    
    register_cache("app_logs", df)
    return df


@pytest.fixture
def sample_error_logs():
    """
    Sample error logs for multi-index union queries.
    Contains: _raw, severity, source, error_code
    """
    df = pd.DataFrame({
        "_raw": [
            "Critical error in payment service",
            "Warning: disk space low",
            "Error: authentication failed",
            "Critical: database unreachable",
            "Warning: high latency detected",
        ],
        "severity": ["CRITICAL", "WARNING", "ERROR", "CRITICAL", "WARNING"],
        "source": ["payment", "system", "auth", "database", "network"],
        "error_code": ["E001", "W001", "E002", "E003", "W002"],
        "host": ["app01", "app02", "app01", "db01", "app03"],
    })
    
    register_cache("error_logs", df)
    return df


@pytest.fixture
def sample_user_events():
    """
    Sample user events for transaction/session analysis.
    Contains: _time, user_id, event_type, page, duration
    """
    np.random.seed(42)
    
    data = []
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    # Create sessions for multiple users
    for user_num in range(1, 6):
        user_id = f"U{user_num:03d}"
        # Each user has multiple sessions
        for session in range(3):
            session_start = base_time + timedelta(hours=user_num + session * 2)
            # Each session has multiple events
            n_events = np.random.randint(3, 8)
            for event in range(n_events):
                data.append({
                    "_time": session_start + timedelta(seconds=event * 30),
                    "user_id": user_id,
                    "event_type": np.random.choice(["pageview", "click", "scroll", "submit"]),
                    "page": np.random.choice(["/home", "/products", "/cart", "/checkout"]),
                    "duration": np.random.randint(1, 60),
                })
    
    df = pd.DataFrame(data)
    register_cache("user_events", df)
    return df


@pytest.fixture
def sample_session_logs():
    """
    Sample session logs for transaction command testing.
    Contains: _time, session_id, user_id, action, page
    """
    np.random.seed(42)
    
    data = []
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    # Create multiple sessions for multiple users
    for user_num in range(1, 6):
        user_id = f"U{user_num:03d}"
        # Each user has multiple sessions
        for session_num in range(3):
            session_id = f"S{user_num:03d}_{session_num:02d}"
            session_start = base_time + timedelta(hours=user_num * 2 + session_num)
            # Each session has multiple events
            n_events = np.random.randint(4, 10)
            for event in range(n_events):
                data.append({
                    "_time": session_start + timedelta(seconds=event * 45),
                    "session_id": session_id,
                    "user_id": user_id,
                    "action": np.random.choice(["view", "click", "scroll", "submit", "navigate"]),
                    "page": np.random.choice(["/home", "/products", "/cart", "/checkout", "/profile"]),
                })
    
    df = pd.DataFrame(data)
    register_cache("session_logs", df)
    return df


@pytest.fixture
def sample_server_metrics_with_response():
    """
    Sample server metrics including response_time for percentile tests.
    Contains: _time, host, cpu_usage, memory_usage, response_time
    """
    np.random.seed(42)
    n = 200
    
    hosts = ["server01", "server02", "server03"]
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    
    data = []
    for i in range(n):
        for host in hosts:
            cpu = np.clip(np.random.normal(40, 15), 0, 100)
            memory = np.clip(np.random.normal(55, 20), 0, 100)
            # Response time with occasional spikes
            response = np.random.exponential(100) if np.random.random() > 0.05 else np.random.exponential(500)
            
            data.append({
                "_time": base_time + timedelta(minutes=i * 5),
                "host": host,
                "cpu_usage": round(cpu, 2),
                "memory_usage": round(memory, 2),
                "response_time": round(response, 2),
            })
    
    df = pd.DataFrame(data)
    register_cache("server_metrics_rt", df)
    return df


def execute_command(cmd: str) -> pd.DataFrame:
    """Helper function to execute a command and return result."""
    return CommandExecutor(cmd).execute()

