"""
MvExpand command - Expand multi-value fields into separate rows.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class MvExpandCommand(PipeCommand):
    """
    MvExpand command for expanding multi-value fields.

    Multi-value fields can be:
    - Lists/arrays
    - Comma-separated strings
    - Other delimiter-separated strings

    Usage:
        mvexpand tags                    # Expand list field 'tags'
        mvexpand categories delim=","    # Expand comma-separated string
        mvexpand items delim="|"         # Expand pipe-separated string
        mvexpand field limit=100         # Limit expansion to 100 rows per original row
    """

    keywords = ["mvexpand", "expand", "explode"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.field: str = kwargs.get("field", "")
        self.delimiter: str | None = kwargs.get("delim")
        self.limit: int | None = kwargs.get("limit")

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse field from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import (
            PositionalArgumentNode,
            KeywordArgumentNode,
            LiteralNode,
            IdentifierNode,
        )

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    self.field = str(arg.value.value).strip('"\'')
                elif isinstance(arg.value, IdentifierNode):
                    self.field = arg.value.name
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    if arg.key in ("delim", "delimiter"):
                        self.delimiter = str(arg.value.value)
                    elif arg.key == "limit":
                        self.limit = int(arg.value.value)

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("delim=") or arg.startswith("delimiter="):
                self.delimiter = arg.split("=", 1)[1].strip('"\'')
            elif arg.startswith("limit="):
                self.limit = int(arg[6:])
            elif not self.field:
                self.field = arg

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the mvexpand operation."""
        if df.empty or not self.field:
            return df

        if self.field not in df.columns:
            raise ValueError(f"Field not found: {self.field}")

        result = df.copy()

        # Handle string fields with delimiter
        if self.delimiter:
            # Split strings by delimiter
            result[self.field] = result[self.field].apply(
                lambda x: str(x).split(self.delimiter) if pd.notna(x) else [x]
            )

        # Expand the field
        result = result.explode(self.field)

        # Apply limit if specified
        if self.limit:
            # Group by original index and take first N
            result = result.groupby(level=0).head(self.limit)

        # Clean up - strip whitespace from expanded string values
        if result[self.field].dtype == object:
            result[self.field] = result[self.field].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )

        return result.reset_index(drop=True)

