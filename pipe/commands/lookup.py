"""
Lookup command - Enrich data by looking up values from another table.

Similar to a left join but specifically designed for enrichment use cases.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.commands.cache import DataFrameCache
from pipe.pipe_map import PipeMap


@PipeMap.register
class LookupCommand(PipeCommand):
    """
    Lookup command for data enrichment.

    Usage:
        lookup table="customers" field=customer_id output=customer_name, customer_email
        lookup table="products" field=product_id
        lookup table="geo" field=ip output=country, city default="Unknown"
    """

    keywords = ["lookup"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.table_name: str = kwargs.get("table", "")
        self.field: str = kwargs.get("field", "")
        self.lookup_field: str = kwargs.get("lookup_field", "")  # Field name in lookup table (if different)
        self.output_fields: list[str] = kwargs.get("output", [])
        self.default_value: Any = kwargs.get("default")

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

        # If lookup_field not specified, use the same as field
        if not self.lookup_field:
            self.lookup_field = self.field

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

        for arg in self._ast_node.arguments:
            if isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    val = arg.value.value
                    if arg.key == "table":
                        self.table_name = str(val)
                    elif arg.key == "field":
                        self.field = str(val)
                    elif arg.key == "lookup_field":
                        self.lookup_field = str(val)
                    elif arg.key == "output":
                        # Could be a comma-separated string
                        self.output_fields = [f.strip() for f in str(val).split(",")]
                    elif arg.key == "default":
                        self.default_value = val
                elif isinstance(arg.value, IdentifierNode):
                    if arg.key == "field":
                        self.field = arg.value.name
                    elif arg.key == "lookup_field":
                        self.lookup_field = arg.value.name

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("table="):
                self.table_name = arg[6:].strip('"\'')
            elif arg.startswith("field="):
                self.field = arg[6:].strip('"\'')
            elif arg.startswith("lookup_field="):
                self.lookup_field = arg[13:].strip('"\'')
            elif arg.startswith("output="):
                output_str = arg[7:].strip('"\'')
                self.output_fields = [f.strip() for f in output_str.split(",")]
            elif arg.startswith("default="):
                self.default_value = arg[8:].strip('"\'')

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the lookup operation."""
        if df.empty:
            return df

        if not self.table_name:
            raise ValueError("Lookup table not specified")

        if not self.field:
            raise ValueError("Lookup field not specified")

        # Get lookup table from cache
        lookup_df = DataFrameCache.get(self.table_name)
        if lookup_df is None:
            raise ValueError(f"Lookup table not found in cache: {self.table_name}")

        if self.field not in df.columns:
            raise ValueError(f"Field not found in source: {self.field}")

        if self.lookup_field not in lookup_df.columns:
            raise ValueError(f"Field not found in lookup table: {self.lookup_field}")

        # Determine output columns
        if self.output_fields:
            # Validate output fields exist in lookup table
            missing = [f for f in self.output_fields if f not in lookup_df.columns]
            if missing:
                raise ValueError(f"Output fields not found in lookup table: {missing}")
            output_cols = self.output_fields
        else:
            # Use all columns except the lookup field
            output_cols = [c for c in lookup_df.columns if c != self.lookup_field]

        # Prepare lookup table with only needed columns
        lookup_subset = lookup_df[[self.lookup_field] + output_cols].drop_duplicates(
            subset=[self.lookup_field], keep="first"
        )

        # Perform left join
        result = df.merge(
            lookup_subset,
            left_on=self.field,
            right_on=self.lookup_field,
            how="left",
            suffixes=("", "_lookup"),
        )

        # Remove duplicate lookup field if different from source field
        if self.lookup_field != self.field and self.lookup_field in result.columns:
            result = result.drop(columns=[self.lookup_field])

        # Fill default values if specified
        if self.default_value is not None:
            for col in output_cols:
                if col in result.columns:
                    result[col] = result[col].fillna(self.default_value)

        return result

