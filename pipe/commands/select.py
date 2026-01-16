"""
Select command - Select specific columns from DataFrame.

Also known as 'fields' or 'table' in some query languages.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class SelectCommand(PipeCommand):
    """
    Select command for choosing specific columns.

    Usage:
        select field1, field2, field3
        fields name, age, email
        table id, name, value

    Supports:
        - Positive selection: select field1, field2 (keep only these)
        - Negative selection: select -field1, -field2 (remove these)
    """

    keywords = ["select", "fields", "table", "project"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.include_fields: list[str] = kwargs.get("include_fields", [])
        self.exclude_fields: list[str] = kwargs.get("exclude_fields", [])

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse fields from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import PositionalArgumentNode, LiteralNode, IdentifierNode

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                field_name = None
                if isinstance(arg.value, LiteralNode):
                    field_name = str(arg.value.value).strip('"\'')
                elif isinstance(arg.value, IdentifierNode):
                    field_name = arg.value.name

                if field_name:
                    if field_name.startswith("-"):
                        self.exclude_fields.append(field_name[1:])
                    else:
                        self.include_fields.append(field_name)

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            field = arg.rstrip(",").strip()
            if field.startswith("-"):
                self.exclude_fields.append(field[1:])
            else:
                self.include_fields.append(field)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the select operation."""
        if df.empty:
            return df

        if self.exclude_fields:
            # Exclude mode: remove specified fields
            cols_to_keep = [c for c in df.columns if c not in self.exclude_fields]
            return df[cols_to_keep]
        elif self.include_fields:
            # Include mode: keep only specified fields
            missing = [f for f in self.include_fields if f not in df.columns]
            if missing:
                raise ValueError(f"Fields not found: {missing}")
            return df[self.include_fields]
        else:
            return df

