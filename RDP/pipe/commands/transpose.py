"""
Transpose command - Transpose DataFrame (swap rows and columns).
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class TransposeCommand(PipeCommand):
    """
    Transpose command for swapping rows and columns.

    Usage:
        transpose
        transpose header_field=name   # Use 'name' column as new headers
        transpose include_header=true # Include original headers as first column
    """

    keywords = ["transpose", "pivot"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.header_field: str | None = kwargs.get("header_field")
        self.include_header: bool = kwargs.get("include_header", True)

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse options from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import KeywordArgumentNode, LiteralNode

        for arg in self._ast_node.arguments:
            if isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    if arg.key == "header_field":
                        self.header_field = str(arg.value.value)
                    elif arg.key == "include_header":
                        self.include_header = str(arg.value.value).lower() in ("true", "1", "yes")

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("header_field="):
                self.header_field = arg[13:].strip('"\'')
            elif arg.startswith("include_header="):
                self.include_header = arg[15:].lower() in ("true", "1", "yes")

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the transpose operation."""
        if df.empty:
            return df

        if self.header_field and self.header_field in df.columns:
            # Use specified column as new headers
            new_headers = df[self.header_field].tolist()
            result = df.drop(columns=[self.header_field]).T
            result.columns = new_headers
        else:
            result = df.T

        if self.include_header:
            # Reset index to make original column names a column
            result = result.reset_index()
            result = result.rename(columns={"index": "field"})
        else:
            result = result.reset_index(drop=True)

        return result

