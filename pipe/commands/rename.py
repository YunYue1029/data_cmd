"""
Rename command - Rename DataFrame columns.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class RenameCommand(PipeCommand):
    """
    Rename command for renaming columns.

    Usage:
        rename old_name as new_name
        rename old1 as new1, old2 as new2
        rename old_name=new_name
    """

    keywords = ["rename"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.renames: dict[str, str] = kwargs.get("renames", {})

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse renames from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import (
            KeywordArgumentNode,
            PositionalArgumentNode,
            LiteralNode,
            IdentifierNode,
            BinaryOpNode,
        )

        for arg in self._ast_node.arguments:
            if isinstance(arg, KeywordArgumentNode):
                # Format: old_name=new_name
                old_name = arg.key
                if isinstance(arg.value, LiteralNode):
                    new_name = str(arg.value.value).strip('"\'')
                elif isinstance(arg.value, IdentifierNode):
                    new_name = arg.value.name
                else:
                    new_name = str(arg.value)
                self.renames[old_name] = new_name

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        i = 0
        while i < len(args):
            arg = args[i].rstrip(",")

            # Check for "old as new" format
            if i + 2 < len(args) and args[i + 1].lower() == "as":
                old_name = arg
                new_name = args[i + 2].rstrip(",")
                self.renames[old_name] = new_name
                i += 3
            # Check for "old=new" format
            elif "=" in arg:
                parts = arg.split("=", 1)
                old_name = parts[0].strip()
                new_name = parts[1].strip().strip('"\'')
                self.renames[old_name] = new_name
                i += 1
            else:
                i += 1

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the rename operation."""
        if df.empty or not self.renames:
            return df

        # Validate that old names exist
        missing = [name for name in self.renames.keys() if name not in df.columns]
        if missing:
            raise ValueError(f"Columns not found: {missing}")

        return df.rename(columns=self.renames)

