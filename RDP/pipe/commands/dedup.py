"""
Dedup command - Remove duplicate rows.
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class DedupCommand(PipeCommand):
    """
    Dedup command for removing duplicate rows.

    Usage:
        dedup                     # Remove duplicates based on all columns
        dedup field1, field2      # Remove duplicates based on specific fields
        dedup field1 sortby=-time # Keep last occurrence (sorted by time desc)
        dedup field1 consecutive=true  # Only remove consecutive duplicates
    """

    keywords = ["dedup", "distinct", "unique"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.fields: list[str] = kwargs.get("fields", [])
        self.keep: str = kwargs.get("keep", "first")  # 'first', 'last', False
        self.consecutive: bool = kwargs.get("consecutive", False)
        self.sortby: list[tuple[str, bool]] = []  # (field, ascending)

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse fields from AST node."""
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
                if arg.key == "keep":
                    if isinstance(arg.value, LiteralNode):
                        self.keep = str(arg.value.value).lower()
                elif arg.key == "consecutive":
                    if isinstance(arg.value, LiteralNode):
                        self.consecutive = str(arg.value.value).lower() in ("true", "1", "yes")
                elif arg.key == "sortby":
                    if isinstance(arg.value, LiteralNode):
                        field_str = str(arg.value.value)
                        if field_str.startswith("-"):
                            self.sortby.append((field_str[1:], False))
                        else:
                            self.sortby.append((field_str, True))

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            arg = arg.rstrip(",")
            if arg.startswith("keep="):
                self.keep = arg[5:].lower()
            elif arg.startswith("consecutive="):
                self.consecutive = arg[12:].lower() in ("true", "1", "yes")
            elif arg.startswith("sortby="):
                field_str = arg[7:]
                if field_str.startswith("-"):
                    self.sortby.append((field_str[1:], False))
                else:
                    self.sortby.append((field_str, True))
            else:
                self.fields.append(arg)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the dedup operation."""
        if df.empty:
            return df

        result = df

        # Sort first if specified
        if self.sortby:
            sort_fields = [f[0] for f in self.sortby]
            ascending = [f[1] for f in self.sortby]
            result = result.sort_values(by=sort_fields, ascending=ascending)

        subset = self.fields if self.fields else None

        if self.consecutive:
            # Remove only consecutive duplicates
            if subset:
                mask = (result[subset] != result[subset].shift()).any(axis=1)
            else:
                mask = (result != result.shift()).any(axis=1)
            result = result[mask]
        else:
            # Remove all duplicates
            result = result.drop_duplicates(subset=subset, keep=self.keep)

        return result.reset_index(drop=True)

