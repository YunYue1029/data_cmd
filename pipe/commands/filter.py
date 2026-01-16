"""
Filter command - Filter DataFrame rows based on conditions.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class FilterCommand(PipeCommand):
    """
    Filter command for selecting rows.

    Usage:
        filter status="active"
        filter amount > 100
        filter category="electronics" status="active"
    """

    keywords = ["filter", "where"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
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

        from syntax_tree.nodes import KeywordArgumentNode, BinaryOpNode, IdentifierNode, LiteralNode

        for arg in self._ast_node.arguments:
            if isinstance(arg, KeywordArgumentNode):
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
        """Parse from args list (legacy format)."""
        for arg in args:
            # Parse field=value or field>value etc.
            for op in [">=", "<=", "!=", "==", ">", "<", "="]:
                if op in arg:
                    parts = arg.split(op, 1)
                    if len(parts) == 2:
                        field = parts[0].strip()
                        value = parts[1].strip()

                        # Remove quotes from value
                        if (value.startswith('"') and value.endswith('"')) or \
                           (value.startswith("'") and value.endswith("'")):
                            value = value[1:-1]
                        else:
                            # Try to convert to number
                            try:
                                value = float(value) if "." in value else int(value)
                            except ValueError:
                                pass

                        # Normalize operator
                        normalized_op = "==" if op == "=" else op

                        self.conditions.append({
                            "field": field,
                            "operator": normalized_op,
                            "value": value,
                        })
                        break

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the filter operation."""
        if df.empty or not self.conditions:
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

