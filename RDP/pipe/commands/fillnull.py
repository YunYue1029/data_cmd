"""
FillNull command - Fill null/missing values in DataFrame.
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class FillNullCommand(PipeCommand):
    """
    FillNull command for filling missing values.

    Usage:
        fillnull value="N/A"                    # Fill all nulls with "N/A"
        fillnull field1, field2 value=0         # Fill specific fields with 0
        fillnull field1 value="unknown"         # Fill specific field
        fillnull method=ffill                   # Forward fill
        fillnull method=bfill                   # Backward fill
        fillnull method=mean                    # Fill with column mean
        fillnull method=median                  # Fill with column median
    """

    keywords = ["fillnull", "fillna", "fill"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.fields: list[str] = kwargs.get("fields", [])
        self.value: Any = kwargs.get("value")
        self.method: str | None = kwargs.get("method")  # ffill, bfill, mean, median, mode

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse fields and value from AST node."""
        if not self._ast_node:
            return

        from RDP.syntax_tree.nodes import (
            PositionalArgumentNode,
            KeywordArgumentNode,
            LiteralNode,
            IdentifierNode,
        )

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    field = str(arg.value.value).strip('"\'')
                    self.fields.append(field)
                elif isinstance(arg.value, IdentifierNode):
                    self.fields.append(arg.value.name)
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    if arg.key == "value":
                        self.value = arg.value.value
                    elif arg.key == "method":
                        self.method = str(arg.value.value).lower()

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            arg = arg.rstrip(",")
            if arg.startswith("value="):
                val = arg[6:].strip('"\'')
                # Try to convert to number
                try:
                    self.value = float(val) if "." in val else int(val)
                except ValueError:
                    self.value = val
            elif arg.startswith("method="):
                self.method = arg[7:].lower()
            else:
                self.fields.append(arg)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the fillnull operation."""
        if df.empty:
            return df

        result = df.copy()
        target_cols = self.fields if self.fields else result.columns.tolist()

        # Validate fields
        missing = [f for f in target_cols if f not in result.columns]
        if missing:
            raise ValueError(f"Fields not found: {missing}")

        if self.method:
            if self.method == "ffill":
                result[target_cols] = result[target_cols].ffill()
            elif self.method == "bfill":
                result[target_cols] = result[target_cols].bfill()
            elif self.method == "mean":
                for col in target_cols:
                    if pd.api.types.is_numeric_dtype(result[col]):
                        result[col] = result[col].fillna(result[col].mean())
            elif self.method == "median":
                for col in target_cols:
                    if pd.api.types.is_numeric_dtype(result[col]):
                        result[col] = result[col].fillna(result[col].median())
            elif self.method == "mode":
                for col in target_cols:
                    mode_val = result[col].mode()
                    if len(mode_val) > 0:
                        result[col] = result[col].fillna(mode_val.iloc[0])
        elif self.value is not None:
            result[target_cols] = result[target_cols].fillna(self.value)

        return result

