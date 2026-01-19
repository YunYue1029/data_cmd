"""
Eval command - Create or modify columns using expressions.

Supports basic arithmetic, string operations, and functions including:
- Math: abs, ceil, floor, round, sqrt, log, log10, exp, pow
- String: lower, upper, trim, ltrim, rtrim, len, substr, replace, split
- Date: year, month, day, hour, minute, second, dayofweek, now
- Time formatting: strftime(time, format), strptime(string, format)
- Type conversion: tonumber, tostring, todate
- Null handling: isnull, isnotnull, coalesce, nullif
- Conditional: if(condition, true_val, false_val), case(cond1, val1, ..., default)
"""

import re
from typing import Any

import pandas as pd
import numpy as np

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class EvalCommand(PipeCommand):
    """
    Eval command for computing new columns.

    Usage:
        eval total = price * quantity
        eval discount_price = price * 0.9
        eval full_name = first_name + " " + last_name
        eval category = if(amount > 100, "high", "low")
        eval year = year(date_field)
        eval upper_name = upper(name)
        eval len = len(description)
    """

    keywords = ["eval", "calculate", "compute"]

    # Built-in functions mapping
    FUNCTIONS = {
        # Math functions
        "abs": np.abs,
        "ceil": np.ceil,
        "floor": np.floor,
        "round": lambda x, n=0: np.round(x, int(n) if not pd.isna(n) else 0),
        "sqrt": np.sqrt,
        "log": np.log,
        "log10": np.log10,
        "exp": np.exp,
        "pow": np.power,
        # String functions
        "lower": lambda x: x.str.lower() if hasattr(x, "str") else str(x).lower(),
        "upper": lambda x: x.str.upper() if hasattr(x, "str") else str(x).upper(),
        "trim": lambda x: x.str.strip() if hasattr(x, "str") else str(x).strip(),
        "ltrim": lambda x: x.str.lstrip() if hasattr(x, "str") else str(x).lstrip(),
        "rtrim": lambda x: x.str.rstrip() if hasattr(x, "str") else str(x).rstrip(),
        "len": lambda x: x.str.len() if hasattr(x, "str") else len(str(x)),
        "substr": lambda x, start, length=None: x.str[int(start):int(start)+int(length)] if length else x.str[int(start):],
        "replace": lambda x, old, new: x.str.replace(old, new, regex=False) if hasattr(x, "str") else str(x).replace(old, new),
        "split": lambda x, sep, idx=0: x.str.split(sep).str[int(idx)] if hasattr(x, "str") else str(x).split(sep)[int(idx)],
        # Date functions
        "year": lambda x: pd.to_datetime(x).dt.year,
        "month": lambda x: pd.to_datetime(x).dt.month,
        "day": lambda x: pd.to_datetime(x).dt.day,
        "hour": lambda x: pd.to_datetime(x).dt.hour,
        "minute": lambda x: pd.to_datetime(x).dt.minute,
        "second": lambda x: pd.to_datetime(x).dt.second,
        "dayofweek": lambda x: pd.to_datetime(x).dt.dayofweek,
        "now": lambda: pd.Timestamp.now(),
        # Time formatting and parsing
        "strftime": lambda x, fmt: pd.to_datetime(x).dt.strftime(fmt),
        "strptime": lambda x, fmt: pd.to_datetime(x, format=fmt, errors="coerce"),
        # Type conversion
        "tonumber": lambda x: pd.to_numeric(x, errors="coerce"),
        "tostring": lambda x: x.astype(str),
        "todate": lambda x: pd.to_datetime(x, errors="coerce"),
        # Null handling
        "isnull": lambda x: pd.isna(x),
        "isnotnull": lambda x: pd.notna(x),
        "coalesce": lambda *args: pd.concat([pd.Series(a) for a in args], axis=1).bfill(axis=1).iloc[:, 0],
        "nullif": lambda x, val: x.where(x != val, np.nan),
    }

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.expressions: list[tuple[str, str]] = []  # (field_name, expression)

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse expressions from AST node."""
        if not self._ast_node:
            return

        from RDP.syntax_tree.nodes import KeywordArgumentNode, LiteralNode, IdentifierNode, BinaryOpNode

        for arg in self._ast_node.arguments:
            if isinstance(arg, KeywordArgumentNode):
                field_name = arg.key
                # Reconstruct expression from AST
                expr = self._ast_to_expr(arg.value)
                self.expressions.append((field_name, expr))

    def _ast_to_expr(self, node: Any) -> str:
        """Convert AST node back to expression string."""
        from RDP.syntax_tree.nodes import LiteralNode, IdentifierNode, BinaryOpNode, FunctionCallNode

        if isinstance(node, LiteralNode):
            if isinstance(node.value, str):
                return f'"{node.value}"'
            return str(node.value)
        elif isinstance(node, IdentifierNode):
            return node.name
        elif isinstance(node, BinaryOpNode):
            left = self._ast_to_expr(node.left)
            right = self._ast_to_expr(node.right)
            return f"({left} {node.operator} {right})"
        elif isinstance(node, FunctionCallNode):
            args = ", ".join(self._ast_to_expr(a) for a in node.arguments)
            return f"{node.name}({args})"
        return str(node)

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        # Join args back and split by comma for multiple expressions
        full_expr = " ".join(args)

        # Split by commas that are not inside parentheses
        exprs = self._split_expressions(full_expr)

        for expr in exprs:
            expr = expr.strip()
            if "=" in expr:
                # Find the first = that's not part of == or !=
                match = re.match(r"(\w+)\s*=\s*(.+)", expr)
                if match:
                    field_name = match.group(1)
                    expression = match.group(2)
                    self.expressions.append((field_name, expression))

    def _split_expressions(self, text: str) -> list[str]:
        """Split expressions by comma, respecting parentheses."""
        result = []
        current = []
        depth = 0

        for char in text:
            if char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                result.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            result.append("".join(current))

        return result

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the eval operation."""
        if df.empty:
            return df

        result = df.copy()

        for field_name, expression in self.expressions:
            value = self._evaluate_expression(expression, result)
            result[field_name] = value

        return result

    def _evaluate_expression(self, expr: str, df: pd.DataFrame) -> pd.Series:
        """Evaluate an expression and return the result."""
        expr = expr.strip()

        # Handle boolean operators (AND, OR) at top level
        bool_result = self._evaluate_boolean_expression(expr, df)
        if bool_result is not None:
            return bool_result

        # Handle if() function specially - use proper parenthesis matching
        if expr.lower().startswith("if(") or expr.lower().startswith("if "):
            args = self._parse_function_call(expr)
            if args and len(args) == 3:
                return self._eval_if(args[0], args[1], args[2], df)

        # Handle case() function
        if expr.lower().startswith("case(") or expr.lower().startswith("case "):
            args = self._parse_function_call(expr)
            if args:
                return self._eval_case_args(args, df)

        # Check for function calls
        func_match = re.match(r"(\w+)\s*\((.+)\)$", expr)
        if func_match:
            func_name = func_match.group(1).lower()
            if func_name in self.FUNCTIONS:
                args_str = func_match.group(2)
                args = self._parse_function_args(args_str, df)
                return self.FUNCTIONS[func_name](*args)

        # Build evaluation context
        context = {"__builtins__": {}, "np": np, "pd": pd}

        # Add DataFrame columns to context
        for col in df.columns:
            context[col] = df[col]

        # Add safe functions
        context.update(self.FUNCTIONS)

        try:
            # Replace field references and evaluate
            result = eval(expr, context)
            if isinstance(result, (int, float, str, bool)):
                return pd.Series([result] * len(df), index=df.index)
            return result
        except Exception as e:
            raise ValueError(f"Failed to evaluate expression '{expr}': {e}")

    def _parse_function_args(self, args_str: str, df: pd.DataFrame) -> list[Any]:
        """Parse function arguments."""
        args = []
        current = []
        depth = 0

        for char in args_str:
            if char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                args.append("".join(current).strip())
                current = []
            else:
                current.append(char)

        if current:
            args.append("".join(current).strip())

        # Evaluate each argument
        result = []
        for arg in args:
            arg = arg.strip()
            if arg.startswith('"') and arg.endswith('"'):
                result.append(arg[1:-1])
            elif arg.startswith("'") and arg.endswith("'"):
                result.append(arg[1:-1])
            elif arg in df.columns:
                result.append(df[arg])
            else:
                try:
                    result.append(float(arg) if "." in arg else int(arg))
                except ValueError:
                    result.append(arg)

        return result

    def _eval_if(self, condition: str, true_val: str, false_val: str, df: pd.DataFrame) -> pd.Series:
        """Evaluate if(condition, true_value, false_value)."""
        # Evaluate condition
        cond_result = self._evaluate_expression(condition, df)

        # Evaluate true and false values
        true_result = self._evaluate_expression(true_val, df)
        false_result = self._evaluate_expression(false_val, df)

        # Handle scalar values
        if isinstance(true_result, (int, float, str, bool)):
            true_result = pd.Series([true_result] * len(df), index=df.index)
        if isinstance(false_result, (int, float, str, bool)):
            false_result = pd.Series([false_result] * len(df), index=df.index)

        return pd.Series(np.where(cond_result, true_result, false_result), index=df.index)

    def _parse_function_call(self, expr: str) -> list[str] | None:
        """
        Parse a function call and return its arguments.
        Properly handles nested parentheses and quoted strings.
        
        Example: if(a > 1, if(b > 2, "x", "y"), "z") -> ["a > 1", "if(b > 2, \"x\", \"y\")", "\"z\""]
        """
        expr = expr.strip()
        
        # Find the opening parenthesis
        paren_start = expr.find("(")
        if paren_start == -1:
            return None
        
        # Find matching closing parenthesis
        depth = 0
        in_string = False
        string_char = None
        
        for i, char in enumerate(expr[paren_start:], paren_start):
            if char in ('"', "'") and (i == 0 or expr[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
            elif not in_string:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                    if depth == 0:
                        # Found matching closing paren
                        args_str = expr[paren_start + 1:i]
                        return self._split_function_args(args_str)
        
        return None

    def _split_function_args(self, args_str: str) -> list[str]:
        """Split function arguments, respecting parentheses and quotes."""
        args = []
        current = []
        depth = 0
        in_string = False
        string_char = None
        
        for i, char in enumerate(args_str):
            if char in ('"', "'") and (i == 0 or args_str[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
                current.append(char)
            elif not in_string:
                if char == "(":
                    depth += 1
                    current.append(char)
                elif char == ")":
                    depth -= 1
                    current.append(char)
                elif char == "," and depth == 0:
                    args.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
            else:
                current.append(char)
        
        if current:
            args.append("".join(current).strip())
        
        return args

    def _eval_case(self, args_str: str, df: pd.DataFrame) -> pd.Series:
        """Evaluate case(cond1, val1, cond2, val2, ..., default)."""
        args = self._split_expressions(args_str)
        return self._eval_case_args(args, df)

    def _eval_case_args(self, args: list[str], df: pd.DataFrame) -> pd.Series:
        """Evaluate case with pre-parsed arguments."""
        result = pd.Series([None] * len(df), index=df.index)

        i = 0
        while i < len(args) - 1:
            condition = args[i].strip()
            value = args[i + 1].strip()

            cond_result = self._evaluate_expression(condition, df)
            val_result = self._evaluate_expression(value, df)

            if isinstance(val_result, (int, float, str, bool)):
                val_result = pd.Series([val_result] * len(df), index=df.index)

            result = result.where(~cond_result | result.notna(), val_result)
            i += 2

        # Handle default value (last argument if odd number)
        if len(args) % 2 == 1:
            default = self._evaluate_expression(args[-1].strip(), df)
            if isinstance(default, (int, float, str, bool)):
                default = pd.Series([default] * len(df), index=df.index)
            result = result.fillna(default)

        return result

    def _evaluate_boolean_expression(self, expr: str, df: pd.DataFrame) -> pd.Series | None:
        """
        Evaluate boolean expressions with AND, OR operators.
        Returns None if expr doesn't contain boolean operators at top level.
        """
        expr = expr.strip()
        
        # Strip outer parentheses if balanced
        if expr.startswith("(") and expr.endswith(")"):
            inner = expr[1:-1]
            if self._is_balanced_parens(inner):
                expr = inner.strip()
        
        # Find AND/OR at top level (not inside parentheses or quotes)
        or_pos = self._find_top_level_operator(expr, " OR ")
        if or_pos is not None:
            left = expr[:or_pos].strip()
            right = expr[or_pos + 4:].strip()
            left_result = self._evaluate_boolean_expression(left, df)
            if left_result is None:
                left_result = self._evaluate_simple_condition(left, df)
            right_result = self._evaluate_boolean_expression(right, df)
            if right_result is None:
                right_result = self._evaluate_simple_condition(right, df)
            return left_result | right_result
        
        and_pos = self._find_top_level_operator(expr, " AND ")
        if and_pos is not None:
            left = expr[:and_pos].strip()
            right = expr[and_pos + 5:].strip()
            left_result = self._evaluate_boolean_expression(left, df)
            if left_result is None:
                left_result = self._evaluate_simple_condition(left, df)
            right_result = self._evaluate_boolean_expression(right, df)
            if right_result is None:
                right_result = self._evaluate_simple_condition(right, df)
            return left_result & right_result
        
        # Check for simple comparison operators
        for op in [">=", "<=", "!=", "==", "=", ">", "<"]:
            op_pos = self._find_top_level_operator(expr, op)
            if op_pos is not None:
                return self._evaluate_simple_condition(expr, df)
        
        return None

    def _is_balanced_parens(self, expr: str) -> bool:
        """Check if parentheses are balanced in expression."""
        depth = 0
        in_string = False
        string_char = None
        
        for i, char in enumerate(expr):
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
                    if depth < 0:
                        return False
        
        return depth == 0

    def _find_top_level_operator(self, expr: str, op: str) -> int | None:
        """Find operator position at top level (not inside parens/quotes)."""
        in_string = False
        string_char = None
        depth = 0
        expr_upper = expr.upper()
        op_upper = op.upper()
        
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
            
            if not in_string and depth == 0 and expr_upper[i:i+len(op)] == op_upper:
                # Check it's not part of a longer operator
                if op == "=" and i > 0 and expr[i-1] in ("!", ">", "<"):
                    continue
                if op == "=" and i + 1 < len(expr) and expr[i+1] == "=":
                    continue
                return i
        
        return None

    def _evaluate_simple_condition(self, expr: str, df: pd.DataFrame) -> pd.Series:
        """Evaluate a simple comparison like 'field = value' or 'field > 10'."""
        expr = expr.strip()
        
        for op, op_str in [(">=", ">="), ("<=", "<="), ("!=", "!="), 
                           ("==", "=="), ("=", "="), (">", ">"), ("<", "<")]:
            op_pos = self._find_top_level_operator(expr, op_str)
            if op_pos is not None:
                left = expr[:op_pos].strip()
                right = expr[op_pos + len(op_str):].strip()
                
                # Get left value - evaluate as expression if complex
                left_val = self._evaluate_operand(left, df)
                
                # Get right value - evaluate as expression if complex
                right_val = self._evaluate_operand(right, df)
                
                # Apply comparison and ensure result is a Series
                if op in ("=", "=="):
                    result = left_val == right_val
                elif op == "!=":
                    result = left_val != right_val
                elif op == ">":
                    result = left_val > right_val
                elif op == "<":
                    result = left_val < right_val
                elif op == ">=":
                    result = left_val >= right_val
                elif op == "<=":
                    result = left_val <= right_val
                
                # Convert scalar bool to Series
                if isinstance(result, bool):
                    result = pd.Series([result] * len(df), index=df.index)
                
                return result
        
        raise ValueError(f"Cannot evaluate condition: {expr}")

    def _evaluate_operand(self, expr: str, df: pd.DataFrame):
        """Evaluate an operand in a comparison - could be field, literal, or expression."""
        expr = expr.strip()
        
        # String literal
        if expr.startswith('"') and expr.endswith('"'):
            return expr[1:-1]
        if expr.startswith("'") and expr.endswith("'"):
            return expr[1:-1]
        
        # Simple field reference
        if expr in df.columns:
            return df[expr]
        
        # Simple number
        try:
            return float(expr) if "." in expr else int(expr)
        except ValueError:
            pass
        
        # Complex expression - contains operators or parentheses
        if "(" in expr or "+" in expr or "-" in expr or "*" in expr or "/" in expr:
            # Use the general expression evaluator, but avoid infinite recursion
            # by using Python's eval with DataFrame context
            context = {"__builtins__": {}, "np": np, "pd": pd}
            for col in df.columns:
                context[col] = df[col]
            context.update(self.FUNCTIONS)
            try:
                return eval(expr, context)
            except Exception:
                pass
        
        # Unknown - return as string
        return expr

