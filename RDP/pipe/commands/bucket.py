"""
Bucket command - Time windowing/binning for time series data.

Supports time span formats:
- Seconds: 30s, 60s
- Minutes: 5m, 10m, 15m
- Hours: 1h, 6h, 12h
- Days: 1d, 7d
"""

import re
from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class BucketCommand(PipeCommand):
    """
    Bucket command for time-based binning.

    Groups time values into discrete buckets/bins for aggregation.

    Usage:
        bucket _time span=5m
        bucket _time span=1h | stats avg(cpu) by _time, host
        bucket timestamp span=30s
    """

    keywords = ["bucket", "bin"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.field: str = kwargs.get("field", "_time")
        self.span: str = kwargs.get("span", "5m")

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse from AST node."""
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
                # Positional argument is the field name
                if isinstance(arg.value, IdentifierNode):
                    self.field = arg.value.name
                elif isinstance(arg.value, LiteralNode):
                    self.field = str(arg.value.value)
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    val = str(arg.value.value)
                elif isinstance(arg.value, IdentifierNode):
                    val = arg.value.name
                else:
                    val = str(arg.value)

                if arg.key == "span":
                    self.span = val
                elif arg.key == "field":
                    self.field = val

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("span="):
                self.span = arg[5:].strip('"\'')
            elif arg.startswith("field="):
                self.field = arg[6:].strip('"\'')
            elif "=" not in arg:
                # Assume it's the field name
                self.field = arg.strip('"\'')

    def _parse_span(self, span: str) -> pd.Timedelta:
        """
        Parse time span string to pandas Timedelta.

        Supports formats:
        - 30s (seconds)
        - 5m, 10m (minutes)
        - 1h, 6h (hours)
        - 1d, 7d (days)
        - 1w (weeks)

        Args:
            span: Time span string

        Returns:
            pd.Timedelta object
        """
        match = re.match(r"^(\d+)([smhdw])$", span.lower())
        if not match:
            raise ValueError(f"Invalid span format: {span}. Use formats like 30s, 5m, 1h, 1d, 1w")

        value = int(match.group(1))
        unit = match.group(2)

        unit_map = {
            "s": "seconds",
            "m": "minutes",
            "h": "hours",
            "d": "days",
            "w": "weeks",
        }

        return pd.Timedelta(**{unit_map[unit]: value})

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the bucket operation."""
        if df.empty:
            return df

        if self.field not in df.columns:
            raise ValueError(f"Field not found: {self.field}")

        result = df.copy()

        # Parse span to timedelta
        span_td = self._parse_span(self.span)

        # Get the time column
        time_col = result[self.field]

        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(time_col):
            try:
                time_col = pd.to_datetime(time_col)
            except Exception as e:
                raise ValueError(f"Cannot convert field '{self.field}' to datetime: {e}")

        # Calculate bucket start times
        # Floor to the nearest span
        span_ns = span_td.value  # nanoseconds
        time_ns = time_col.astype("int64")
        bucketed_ns = (time_ns // span_ns) * span_ns
        result[self.field] = pd.to_datetime(bucketed_ns)

        return result

