# data-cmd

A Splunk SPL-inspired data processing pipeline for pandas DataFrames.

## Overview

`data-cmd` provides a command-line style query language for processing pandas DataFrames, similar to Splunk's Search Processing Language (SPL). It allows you to chain multiple data operations using a familiar pipe (`|`) syntax.

## Features

- **Pipe-based Command Chaining**: Chain multiple commands with `|` operator
- **Splunk-like Syntax**: Familiar syntax for users coming from Splunk
- **Query Optimization**: Built-in query planner with optimization support
- **Extensible Architecture**: Easy to add custom commands
- **20+ Built-in Commands**: Comprehensive set of data processing commands

## Requirements

- Python >= 3.11
- pandas >= 2.0.0

## Quick Start

```python
import pandas as pd
from executors import CommandExecutor, register_cache

# Create sample data
df = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
    "department": ["Sales", "IT", "IT", "Sales", "IT"],
    "amount": [100, 200, 150, 300, 250]
})

# Register DataFrame to cache
register_cache("sales", df)

# Execute a command pipeline
result = CommandExecutor(
    'cache=sales | stats sum(amount) as total, count as n by department'
).execute()

print(result)
#   department  total  n
# 0         IT    600  3
# 1      Sales    400  2
```

## Available Commands

### Data Retrieval
| Command | Description | Example |
|---------|-------------|---------|
| `cache` | Load data from cache | `cache=my_data` |
| `search` | Search from an index | `search index="logs"` |
| `lookup` | Lookup from another dataset | `lookup users user_id` |

### Row Selection
| Command | Description | Example |
|---------|-------------|---------|
| `filter` | Filter rows by condition | `filter status == "active"` |
| `head` | Get first N rows | `head 10` |
| `tail` | Get last N rows | `tail 10` |
| `sample` | Random sample of rows | `sample 100` |
| `dedup` | Remove duplicate rows | `dedup user_id` |
| `dropnull` | Remove rows with null values | `dropnull field1, field2` |

### Column Operations
| Command | Description | Example |
|---------|-------------|---------|
| `select` | Select specific columns | `select name, amount` |
| `rename` | Rename columns | `rename old_name as new_name` |
| `eval` | Create/modify columns with expressions | `eval total = price * quantity` |

### Aggregation
| Command | Description | Example |
|---------|-------------|---------|
| `stats` | Aggregate statistics | `stats sum(amount), count by category` |
| `top` | Most common values | `top 10 category` |
| `rare` | Least common values | `rare 10 category` |

### Transformation
| Command | Description | Example |
|---------|-------------|---------|
| `sort` | Sort rows | `sort -amount` (descending) |
| `reverse` | Reverse row order | `reverse` |
| `transpose` | Transpose rows and columns | `transpose` |
| `fillnull` | Fill null values | `fillnull value=0 amount` |
| `replace` | Replace values | `replace "old" with "new" in field` |
| `mvexpand` | Expand multi-value fields | `mvexpand tags` |
| `rex` | Extract fields using regex | `rex field=msg "user=(?<user>\w+)"` |

### Join Operations
| Command | Description | Example |
|---------|-------------|---------|
| `join` | Join with another dataset | `join user_id [search index="users"]` |

## Command Pipeline Examples

```python
# Complex pipeline with multiple operations
cmd = '''
cache=orders 
| join customer_id [search index="customers" | stats first(name) as name by customer_id]
| filter amount > 100
| stats sum(amount) as total, count as orders by name
| sort -total
| head 10
'''
result = CommandExecutor(cmd).execute()

# Using stats with multiple aggregations
cmd = 'cache=logs | stats count, avg(duration) as avg_time, max(duration) as max_time by endpoint'
result = CommandExecutor(cmd).execute()

# Data cleaning pipeline
cmd = '''
cache=raw_data
| dropnull email
| dedup user_id
| fillnull value="unknown" department
| eval full_name = first_name + " " + last_name
'''
result = CommandExecutor(cmd).execute()
```

## Architecture

```
data-cmd/
├── lexer.py              # Tokenizer for command strings
├── parser/
│   ├── command_parser.py # Parse command pipeline
│   └── expression_parser.py
├── syntax_tree/
│   ├── nodes.py          # AST node definitions
│   └── transformer.py
├── planner/
│   ├── query_planner.py  # Query planning
│   └── optimizers.py     # Query optimization
├── pipe/
│   ├── commands/         # Command implementations
│   │   ├── base.py
│   │   ├── stats.py
│   │   ├── filter.py
│   │   └── ...
│   ├── pipe_map.py       # Command registry
│   └── services.py       # Pipeline execution
└── executors.py          # Main entry point
```
