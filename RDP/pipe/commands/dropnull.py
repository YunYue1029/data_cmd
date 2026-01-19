"""
DropNull command - Remove rows with null/missing values.
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class DropNullCommand(PipeCommand):
    """
    DropNull command for removing rows with missing values.

    Usage:
        dropnull                   # Drop rows where ANY column is null
        dropnull field1, field2    # Drop rows where specified fields are null
        dropnull how=all           # Drop rows where ALL columns are null
        dropnull thresh=3          # Drop rows with less than 3 non-null values
    """

    keywords = ["dropnull", "dropna", "drop_null"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.fields: list[str] = kwargs.get("fields", [])
        self.how: str = kwargs.get("how", "any")  # 'any' or 'all'
        self.thresh: int | None = kwargs.get("thresh")

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse fields from AST node."""
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
                    field = str(arg.value.value).strip('"\'')
                    self.fields.append(field)
                elif isinstance(arg.value, IdentifierNode):
                    self.fields.append(arg.value.name)
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    if arg.key == "how":
                        self.how = str(arg.value.value).lower()
                    elif arg.key == "thresh":
                        self.thresh = int(arg.value.value)

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            arg = arg.rstrip(",")
            if arg.startswith("how="):
                self.how = arg[4:].lower()
            elif arg.startswith("thresh="):
                self.thresh = int(arg[7:])
            else:
                self.fields.append(arg)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the dropnull operation."""
        if df.empty:
            return df

        subset = self.fields if self.fields else None

        # Validate fields
        if subset:
            missing = [f for f in subset if f not in df.columns]
            if missing:
                raise ValueError(f"Fields not found: {missing}")

        if self.thresh is not None:
            result = df.dropna(subset=subset, thresh=self.thresh)
        else:
            result = df.dropna(subset=subset, how=self.how)

        return result.reset_index(drop=True)

