"""
Filter command - Filter DataFrame rows based on conditions.

Supports:
- Basic comparisons: =, ==, !=, >, <, >=, <=
- Boolean operators: AND, OR, NOT
- Pattern matching: LIKE with % wildcard
- List membership: IN, NOT IN
- Null checks: isnull(), isnotnull()
- Parentheses for grouping
"""

import re
from typing import Any

import pandas as pd
import numpy as np

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class FilterCommand(PipeCommand):
    """
    Filter command for selecting rows.

    Usage:
        filter status="active"
        where amount > 100
        where status_code >= 400 AND status_code < 500
        where host = "web01" OR host = "web02"
        where NOT is_error = 1
        where uri LIKE "%api%"
        where status_code IN (200, 201, 404)
        where isnull(value)
    """

    keywords = ["filter", "where"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.expression: str = ""
        self.conditions: list[dict[str, Any]] = kwargs.get("conditions", [])

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse conditions from AST node."""
        if not self._ast_node:
            return

        from RDP.syntax_tree.nodes import (
            KeywordArgumentNode, BinaryOpNode, IdentifierNode, 
            LiteralNode, PositionalArgumentNode
        )

        # Check if we have a raw expression string (from where command)
        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    # This is a raw expression string from where command
                    expr = str(arg.value.value)
                    if self.expression:
                        self.expression += " " + expr
                    else:
                        self.expression = expr
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, BinaryOpNode):
                    op_node = arg.value
                    field = arg.key
                    operator = op_node.operator

                    # Get value from right side
                    if isinstance(op_node.right, LiteralNode):
                        value = op_node.right.value
                    elif isinstance(op_node.right, IdentifierNode):
                        value = op_node.right.name
                    else:
                        value = str(op_node.right)

                    self.conditions.append({
                        "field": field,
                        "operator": operator,
                        "value": value,
                    })

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list - store full expression for complex parsing."""
        self.expression = " ".join(args)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the filter operation."""
        if df.empty:
            return df

        # If we have a complex expression, evaluate it
        if self.expression:
            mask = self._evaluate_expression(self.expression, df)
            return df[mask].reset_index(drop=True)

        # Legacy: simple conditions list
        if not self.conditions:
            return df

        result = df
        for cond in self.conditions:
            field = cond["field"]
            op = cond["operator"]
            value = cond["value"]

            if field not in result.columns:
                raise ValueError(f"Field not found: {field}")

            if op in ("=", "=="):
                result = result[result[field] == value]
            elif op == "!=":
                result = result[result[field] != value]
            elif op == ">":
                result = result[result[field] > value]
            elif op == "<":
                result = result[result[field] < value]
            elif op == ">=":
                result = result[result[field] >= value]
            elif op == "<=":
                result = result[result[field] <= value]

        return result.reset_index(drop=True)

    def _evaluate_expression(self, expr: str, df: pd.DataFrame) -> pd.Series:
        """
        Evaluate a boolean expression and return a boolean Series.
        
        Supports: AND, OR, NOT, comparisons, LIKE, IN, isnull, isnotnull
        """
        expr = expr.strip()
        
        # Handle OR (lowest precedence)
        or_parts = self._split_by_operator(expr, " OR ")
        if len(or_parts) > 1:
            result = pd.Series([False] * len(df), index=df.index)
            for part in or_parts:
                result = result | self._evaluate_expression(part.strip(), df)
            return result

        # Handle AND
        and_parts = self._split_by_operator(expr, " AND ")
        if len(and_parts) > 1:
            result = pd.Series([True] * len(df), index=df.index)
            for part in and_parts:
                result = result & self._evaluate_expression(part.strip(), df)
            return result

        # Handle NOT
        if expr.upper().startswith("NOT "):
            inner = expr[4:].strip()
            return ~self._evaluate_expression(inner, df)

        # Handle parentheses
        if expr.startswith("(") and expr.endswith(")"):
            # Check if these are matching outer parentheses
            depth = 0
            for i, c in enumerate(expr):
                if c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
                if depth == 0 and i < len(expr) - 1:
                    # Parentheses don't wrap the whole expression
                    break
            else:
                # They do wrap the whole expression
                return self._evaluate_expression(expr[1:-1], df)

        # Handle NOT IN
        not_in_match = re.match(
            r"(\w+)\s+NOT\s+IN\s*\(([^)]+)\)",
            expr,
            re.IGNORECASE
        )
        if not_in_match:
            field = not_in_match.group(1)
            values_str = not_in_match.group(2)
            values = self._parse_value_list(values_str)
            if field not in df.columns:
                raise ValueError(f"Field not found: {field}")
            return ~df[field].isin(values)

        # Handle IN
        in_match = re.match(
            r"(\w+)\s+IN\s*\(([^)]+)\)",
            expr,
            re.IGNORECASE
        )
        if in_match:
            field = in_match.group(1)
            values_str = in_match.group(2)
            values = self._parse_value_list(values_str)
            if field not in df.columns:
                raise ValueError(f"Field not found: {field}")
            return df[field].isin(values)

        # Handle LIKE (with or without quotes)
        like_match = re.match(
            r'(\w+)\s+LIKE\s+(?:["\']([^"\']+)["\']|(\S+))',
            expr,
            re.IGNORECASE
        )
        if like_match:
            field = like_match.group(1)
            # Pattern can be in group 2 (with quotes) or group 3 (without quotes)
            pattern = like_match.group(2) or like_match.group(3)
            if field not in df.columns:
                raise ValueError(f"Field not found: {field}")
            # Convert SQL LIKE pattern to regex
            regex_pattern = self._like_to_regex(pattern)
            return df[field].astype(str).str.match(regex_pattern, na=False)

        # Handle isnull(field)
        isnull_match = re.match(r"isnull\s*\(\s*(\w+)\s*\)", expr, re.IGNORECASE)
        if isnull_match:
            field = isnull_match.group(1)
            if field not in df.columns:
                raise ValueError(f"Field not found: {field}")
            return pd.isna(df[field])

        # Handle isnotnull(field)
        isnotnull_match = re.match(r"isnotnull\s*\(\s*(\w+)\s*\)", expr, re.IGNORECASE)
        if isnotnull_match:
            field = isnotnull_match.group(1)
            if field not in df.columns:
                raise ValueError(f"Field not found: {field}")
            return pd.notna(df[field])

        # Handle comparison operators
        return self._evaluate_comparison(expr, df)

    def _split_by_operator(self, expr: str, operator: str) -> list[str]:
        """
        Split expression by operator, respecting parentheses and quotes.
        """
        parts = []
        current = []
        depth = 0
        in_string = False
        string_char = None
        i = 0
        op_upper = operator.upper()
        expr_upper = expr.upper()

        while i < len(expr):
            char = expr[i]

            # Track string state
            if char in ('"', "'") and (i == 0 or expr[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            # Track parentheses
            if not in_string:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1

            # Check for operator at this position
            if (not in_string and depth == 0 and 
                expr_upper[i:i+len(operator)] == op_upper):
                # Found operator at top level
                parts.append("".join(current))
                current = []
                i += len(operator)
                continue

            current.append(char)
            i += 1

        if current:
            parts.append("".join(current))

        return parts

    # Supported functions for filter expressions
    FILTER_FUNCTIONS = {
        "abs": np.abs,
        "len": lambda x: x.str.len() if hasattr(x, "str") else len(str(x)),
        "lower": lambda x: x.str.lower() if hasattr(x, "str") else str(x).lower(),
        "upper": lambda x: x.str.upper() if hasattr(x, "str") else str(x).upper(),
        "round": lambda x, n=0: np.round(x, int(n) if not pd.isna(n) else 0),
        "floor": np.floor,
        "ceil": np.ceil,
        "sqrt": np.sqrt,
    }

    def _evaluate_function_call(self, expr: str, df: pd.DataFrame) -> pd.Series | None:
        """
        Evaluate a function call expression.
        Returns None if expr is not a function call.
        """
        # Match function call pattern: func_name(args)
        func_match = re.match(r"(\w+)\s*\(\s*(.+?)\s*\)$", expr.strip())
        if not func_match:
            return None
        
        func_name = func_match.group(1).lower()
        args_str = func_match.group(2)
        
        if func_name not in self.FILTER_FUNCTIONS:
            return None
        
        # Parse arguments
        args = []
        for arg in args_str.split(","):
            arg = arg.strip()
            if arg in df.columns:
                args.append(df[arg])
            elif arg.startswith('"') and arg.endswith('"'):
                args.append(arg[1:-1])
            elif arg.startswith("'") and arg.endswith("'"):
                args.append(arg[1:-1])
            else:
                try:
                    args.append(float(arg) if "." in arg else int(arg))
                except ValueError:
                    args.append(arg)
        
        return self.FILTER_FUNCTIONS[func_name](*args)

    def _evaluate_comparison(self, expr: str, df: pd.DataFrame) -> pd.Series:
        """Evaluate a simple comparison expression."""
        # Try each operator in order of precedence
        for op, op_str in [
            (">=", ">="), ("<=", "<="), ("!=", "!="), 
            ("==", "=="), ("=", "="), (">", ">"), ("<", "<")
        ]:
            # Find operator not inside quotes and parentheses
            match = self._find_operator_outside_parens(expr, op_str)
            if match:
                left = expr[:match].strip()
                right = expr[match + len(op_str):].strip()
                
                # Get left value - could be field name or function call
                left_series = self._get_value_series(left, df)
                
                # Parse right value
                right_val = self._parse_value(right, df)
                
                # Apply comparison
                if op in ("=", "=="):
                    return left_series == right_val
                elif op == "!=":
                    return left_series != right_val
                elif op == ">":
                    return left_series > right_val
                elif op == "<":
                    return left_series < right_val
                elif op == ">=":
                    return left_series >= right_val
                elif op == "<=":
                    return left_series <= right_val

        raise ValueError(f"Cannot parse expression: {expr}")

    def _get_value_series(self, expr: str, df: pd.DataFrame) -> pd.Series:
        """Get a Series from expression - field name or function call."""
        expr = expr.strip()
        
        # Try as function call first
        func_result = self._evaluate_function_call(expr, df)
        if func_result is not None:
            return func_result
        
        # Try as field name
        if expr in df.columns:
            return df[expr]
        
        raise ValueError(f"Field not found: {expr}")

    def _find_operator_outside_parens(self, expr: str, op: str) -> int | None:
        """Find operator position, not inside quotes or parentheses."""
        in_string = False
        string_char = None
        depth = 0
        
        for i in range(len(expr) - len(op) + 1):
            char = expr[i]
            
            if char in ('"', "'") and (i == 0 or expr[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            if not in_string:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
            
            if not in_string and depth == 0 and expr[i:i+len(op)] == op:
                # Check it's not part of a longer operator
                if op == "=" and i > 0 and expr[i-1] in ("!", ">", "<"):
                    continue
                if op == "=" and i + 1 < len(expr) and expr[i+1] == "=":
                    continue
                return i
        
        return None

    def _find_operator(self, expr: str, op: str) -> int | None:
        """Find operator position, not inside quotes."""
        in_string = False
        string_char = None
        
        for i in range(len(expr) - len(op) + 1):
            char = expr[i]
            
            if char in ('"', "'") and (i == 0 or expr[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            
            if not in_string and expr[i:i+len(op)] == op:
                return i
        
        return None

    def _parse_value(self, value_str: str, df: pd.DataFrame) -> Any:
        """Parse a value from string representation."""
        value_str = value_str.strip()
        
        # String literal
        if ((value_str.startswith('"') and value_str.endswith('"')) or
            (value_str.startswith("'") and value_str.endswith("'"))):
            return value_str[1:-1]
        
        # Try as number
        try:
            if "." in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass
        
        # Field reference
        if value_str in df.columns:
            return df[value_str]
        
        # Return as string
        return value_str

    def _parse_value_list(self, values_str: str) -> list[Any]:
        """Parse a comma-separated list of values."""
        values = []
        for v in values_str.split(","):
            v = v.strip()
            if ((v.startswith('"') and v.endswith('"')) or
                (v.startswith("'") and v.endswith("'"))):
                values.append(v[1:-1])
            else:
                try:
                    if "." in v:
                        values.append(float(v))
                    else:
                        values.append(int(v))
                except ValueError:
                    values.append(v)
        return values

    def _like_to_regex(self, pattern: str) -> str:
        """Convert SQL LIKE pattern to regex."""
        # Escape regex special characters except % and _
        result = ""
        for c in pattern:
            if c == "%":
                result += ".*"
            elif c == "_":
                result += "."
            elif c in r"\.^$+?{}[]|()":
                result += "\\" + c
            else:
                result += c
        return "^" + result + "$"
