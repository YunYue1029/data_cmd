"""
Sort command - Sort DataFrame by specified fields.

Prefix field name with '-' for descending order.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class SortCommand(PipeCommand):
    """
    Sort command.

    Usage:
        sort field1, field2      # Ascending
        sort -field1, field2     # field1 descending, field2 ascending
        sort -total_sales        # Descending by total_sales
    """

    keywords = ["sort"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.sort_fields: list[tuple[str, bool]] = []  # (field, ascending)

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse sort fields from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import PositionalArgumentNode, LiteralNode, IdentifierNode

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    field_str = str(arg.value.value)
                    # Remove quotes if present
                    field_str = field_str.strip('"\'')
                    if field_str.startswith("-"):
                        self.sort_fields.append((field_str[1:], False))  # Descending
                    else:
                        self.sort_fields.append((field_str, True))  # Ascending
                elif isinstance(arg.value, IdentifierNode):
                    field_str = arg.value.name
                    self.sort_fields.append((field_str, True))  # Ascending

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            # Remove trailing comma
            field = arg.rstrip(",")
            if field.startswith("-"):
                self.sort_fields.append((field[1:], False))  # Descending
            else:
                self.sort_fields.append((field, True))  # Ascending

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the sort operation."""
        if df.empty or not self.sort_fields:
            return df

        fields = [f[0] for f in self.sort_fields]
        ascending = [f[1] for f in self.sort_fields]

        # Validate fields exist
        missing = [f for f in fields if f not in df.columns]
        if missing:
            raise ValueError(f"Sort fields not found: {missing}")

        return df.sort_values(by=fields, ascending=ascending).reset_index(drop=True)

