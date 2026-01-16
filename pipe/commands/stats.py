"""
Stats command - Perform aggregation operations.

Supports aggregation functions:
- count, count(field)
- sum(field), avg(field), mean(field)
- min(field), max(field)
- values(field) - list of unique values
- first(field), last(field)
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class StatsCommand(PipeCommand):
    """
    Stats command for aggregation.

    Usage:
        stats count by field
        stats sum(amount) as total, count as n by category
        stats values(segment) as segment by customer_id
    """

    keywords = ["stats", "eventstats"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.aggregations: list[dict[str, Any]] = kwargs.get("aggregations", [])
        self.by_fields: list[str] = kwargs.get("by_fields", [])

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse aggregations from AST node."""
        if not self._ast_node:
            return

        self.by_fields = list(self._ast_node.by_fields)

        for agg in self._ast_node.aggregations:
            # Parse function name and alias
            name_parts = agg.name.split(":")
            func_name = name_parts[0]
            alias = name_parts[1] if len(name_parts) > 1 else None

            # Get field from arguments
            field = None
            if agg.arguments:
                from syntax_tree.nodes import IdentifierNode, LiteralNode
                arg = agg.arguments[0]
                if isinstance(arg, IdentifierNode):
                    field = arg.name
                elif isinstance(arg, LiteralNode):
                    field = str(arg.value)

            self.aggregations.append({
                "function": func_name.lower(),
                "field": field,
                "alias": alias,
            })

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list (legacy format)."""
        i = 0
        while i < len(args):
            arg = args[i]

            # Check for 'by' keyword
            if arg.lower() == "by":
                i += 1
                while i < len(args):
                    field = args[i].rstrip(",")
                    self.by_fields.append(field)
                    i += 1
                break

            # Parse aggregation
            agg = self._parse_aggregation(arg)
            if agg:
                self.aggregations.append(agg)

            i += 1

    def _parse_aggregation(self, arg: str) -> dict[str, Any] | None:
        """Parse a single aggregation expression."""
        arg = arg.rstrip(",")

        # Check for alias (e.g., "sum(amount) as total")
        alias = None
        if " as " in arg.lower():
            parts = arg.lower().split(" as ")
            arg = parts[0].strip()
            alias = parts[1].strip()

        # Parse function call (e.g., "count(field)" or just "count")
        if "(" in arg:
            paren_idx = arg.index("(")
            func_name = arg[:paren_idx].lower()
            field = arg[paren_idx + 1 : -1].strip() if arg.endswith(")") else None
            return {"function": func_name, "field": field or None, "alias": alias}
        else:
            # Simple aggregation (e.g., "count")
            return {"function": arg.lower(), "field": None, "alias": alias}

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the stats aggregation."""
        if df.empty:
            return df

        # Build aggregation specifications
        agg_specs: list[tuple[str, str, str, Any]] = []  # (alias, field, func_name, func)

        for agg in self.aggregations:
            func = agg["function"]
            field_name = agg["field"]
            alias = agg["alias"] or self._default_alias(func, field_name)

            if func == "count":
                if field_name:
                    agg_specs.append((alias, field_name, "count", "count"))
                else:
                    # Count all rows - use first column
                    first_col = df.columns[0]
                    agg_specs.append((alias, first_col, "count", "count"))
            elif func == "sum":
                agg_specs.append((alias, field_name, "sum", "sum"))
            elif func in ("avg", "mean"):
                agg_specs.append((alias, field_name, "mean", "mean"))
            elif func == "min":
                agg_specs.append((alias, field_name, "min", "min"))
            elif func == "max":
                agg_specs.append((alias, field_name, "max", "max"))
            elif func == "first":
                agg_specs.append((alias, field_name, "first", "first"))
            elif func == "last":
                agg_specs.append((alias, field_name, "last", "last"))
            elif func == "values":
                # Collect unique values as a list
                agg_specs.append((alias, field_name, "values", lambda x: list(x.unique())))
            elif func in ("dc", "distinct_count"):
                agg_specs.append((alias, field_name, "nunique", "nunique"))
            else:
                raise ValueError(f"Unknown aggregation function: {func}")

        # Perform aggregation
        if self.by_fields:
            # Group by fields - use named aggregation approach
            grouped = df.groupby(self.by_fields, as_index=False)

            # Start with the group keys
            result = grouped[self.by_fields].first()

            # Apply each aggregation
            for alias, field_name, func_name, agg_func in agg_specs:
                if callable(agg_func):
                    agg_result = grouped[field_name].agg(agg_func)
                else:
                    agg_result = grouped[field_name].agg(agg_func)

                # Extract the aggregated column (last column in the result)
                if isinstance(agg_result, pd.DataFrame):
                    result[alias] = agg_result.iloc[:, -1].values
                else:
                    result[alias] = agg_result.values
        else:
            # Aggregate entire DataFrame
            result_dict: dict[str, Any] = {}
            for alias, field_name, func_name, agg_func in agg_specs:
                if callable(agg_func):
                    result_dict[alias] = agg_func(df[field_name])
                else:
                    result_dict[alias] = df[field_name].agg(agg_func)
            result = pd.DataFrame([result_dict])

        return result

    def _default_alias(self, func: str, field: str | None) -> str:
        """Generate default alias for aggregation."""
        if field:
            return f"{func}_{field}"
        return func

