"""
Rare command - Find the least common values in a field.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class RareCommand(PipeCommand):
    """
    Rare command for finding least frequent values.

    Usage:
        rare 10 field                   # 10 least common values in field
        rare field                      # 10 (default) least common values
        rare 5 field by category        # 5 rarest per category
        rare 10 field1, field2          # 10 rarest combinations
        rare 10 field showcount=false   # Don't show count column
        rare 10 field showperc=true     # Show percentage column
    """

    keywords = ["rare"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.limit: int = kwargs.get("limit", 10)
        self.fields: list[str] = kwargs.get("fields", [])
        self.by_fields: list[str] = kwargs.get("by_fields", [])
        self.show_count: bool = kwargs.get("showcount", True)
        self.show_perc: bool = kwargs.get("showperc", False)

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import (
            PositionalArgumentNode,
            KeywordArgumentNode,
            LiteralNode,
            IdentifierNode,
        )

        parsing_by = False
        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    val = arg.value.value
                    if isinstance(val, (int, float)) and not self.fields:
                        self.limit = int(val)
                    elif str(val).lower() == "by":
                        parsing_by = True
                    elif parsing_by:
                        self.by_fields.append(str(val).strip('"\''))
                    else:
                        self.fields.append(str(val).strip('"\''))
                elif isinstance(arg.value, IdentifierNode):
                    name = arg.value.name
                    if name.lower() == "by":
                        parsing_by = True
                    elif parsing_by:
                        self.by_fields.append(name)
                    else:
                        self.fields.append(name)
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    if arg.key == "limit":
                        self.limit = int(arg.value.value)
                    elif arg.key == "showcount":
                        self.show_count = str(arg.value.value).lower() in ("true", "1", "yes")
                    elif arg.key == "showperc":
                        self.show_perc = str(arg.value.value).lower() in ("true", "1", "yes")

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        i = 0
        parsing_by = False

        while i < len(args):
            arg = args[i].rstrip(",")

            if arg.lower() == "by":
                parsing_by = True
            elif arg.startswith("showcount="):
                self.show_count = arg[10:].lower() in ("true", "1", "yes")
            elif arg.startswith("showperc="):
                self.show_perc = arg[9:].lower() in ("true", "1", "yes")
            elif arg.startswith("limit="):
                self.limit = int(arg[6:])
            elif parsing_by:
                self.by_fields.append(arg)
            else:
                try:
                    if not self.fields:
                        self.limit = int(arg)
                    else:
                        self.fields.append(arg)
                except ValueError:
                    self.fields.append(arg)
            i += 1

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the rare operation."""
        if df.empty or not self.fields:
            return df

        # Validate fields
        all_fields = self.fields + self.by_fields
        missing = [f for f in all_fields if f not in df.columns]
        if missing:
            raise ValueError(f"Fields not found: {missing}")

        if self.by_fields:
            # Rare N per group
            results = []
            for _, group in df.groupby(self.by_fields):
                counts = group.groupby(self.fields).size().reset_index(name="count")
                counts = counts.sort_values("count", ascending=True).head(self.limit)
                # Add group identifiers
                for field in self.by_fields:
                    counts[field] = group[field].iloc[0]
                results.append(counts)
            result = pd.concat(results, ignore_index=True)
        else:
            # Rare N overall
            counts = df.groupby(self.fields).size().reset_index(name="count")
            result = counts.sort_values("count", ascending=True).head(self.limit)

        # Add percentage if requested
        if self.show_perc:
            total = result["count"].sum()
            result["percent"] = (result["count"] / total * 100).round(2)

        # Remove count column if not wanted
        if not self.show_count:
            result = result.drop(columns=["count"])

        # Reorder columns: by_fields first, then fields, then count/percent
        col_order = self.by_fields + self.fields
        if self.show_count:
            col_order.append("count")
        if self.show_perc:
            col_order.append("percent")
        result = result[col_order]

        return result.reset_index(drop=True)

