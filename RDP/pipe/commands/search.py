"""
Search command - Retrieve data from an index/cache with optional time filtering.

This is used as a source command for subqueries.
In this implementation, "index" refers to a cached DataFrame.

Supports time range filtering:
- latest=-5m (relative time from now)
- earliest="2024-01-01" (absolute time)
- earliest=-1h latest=-5m (time range)
"""

import re
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.commands.cache import DataFrameCache
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class SearchCommand(PipeCommand):
    """
    Search command for retrieving data with optional time filtering.

    This is primarily used in subqueries to fetch data from
    a named source (cache/index).

    Usage:
        search index="my_data"
        search index=my_data
        search latest=-5m
        search earliest="2024-01-01 15:00:00"
        search earliest="2024-01-10" latest="2024-01-20"
    """

    keywords = ["search"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.index_name = kwargs.get("index", "")
        self.latest: str | None = kwargs.get("latest")
        self.earliest: str | None = kwargs.get("earliest")
        self.time_field: str = kwargs.get("time_field", "_time")

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
            if isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    val = str(arg.value.value)
                elif isinstance(arg.value, IdentifierNode):
                    val = arg.value.name
                else:
                    val = str(arg.value)

                if arg.key == "index":
                    self.index_name = val
                elif arg.key == "latest":
                    self.latest = val
                elif arg.key == "earliest":
                    self.earliest = val
                elif arg.key == "time_field":
                    self.time_field = val

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("index="):
                value = arg[6:]
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                self.index_name = value
            elif arg.startswith("latest="):
                value = arg[7:]
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                self.latest = value
            elif arg.startswith("earliest="):
                value = arg[9:]
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                self.earliest = value
            elif arg.startswith("time_field="):
                value = arg[11:]
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                self.time_field = value

    def _parse_time_spec(self, time_spec: str) -> datetime:
        """
        Parse a time specification to datetime.

        Supports:
        - Relative time: -5m, -1h, -1d, -30s
        - Absolute time: "2024-01-01", "2024-01-01 15:00:00"

        Args:
            time_spec: Time specification string

        Returns:
            datetime object
        """
        # Check for relative time (starts with - or +)
        relative_match = re.match(r"^([+-]?)(\d+)([smhdw])$", time_spec.lower())
        if relative_match:
            sign = relative_match.group(1) or "-"
            value = int(relative_match.group(2))
            unit = relative_match.group(3)

            unit_map = {
                "s": timedelta(seconds=value),
                "m": timedelta(minutes=value),
                "h": timedelta(hours=value),
                "d": timedelta(days=value),
                "w": timedelta(weeks=value),
            }

            delta = unit_map[unit]
            now = datetime.now()

            if sign == "-":
                return now - delta
            else:
                return now + delta

        # Try to parse as absolute datetime
        # Try multiple formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(time_spec, fmt)
            except ValueError:
                continue

        raise ValueError(f"Cannot parse time specification: {time_spec}")

    @classmethod
    def from_source_name(cls, name: str, params: dict[str, Any] | None = None) -> "SearchCommand":
        """Create SearchCommand from source specification."""
        return cls(index=name)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Retrieve DataFrame from the specified index with optional time filtering.

        The "index" is actually a cache key in this implementation.
        """
        # Get data from index if specified
        if self.index_name:
            cached = DataFrameCache.get(self.index_name)
            if cached is None:
                raise ValueError(f"Index/cache not found: {self.index_name}")
            result = cached.copy()
        else:
            result = df.copy() if not df.empty else df

        # Apply time filtering if specified
        if result.empty:
            return result

        if self.earliest is not None or self.latest is not None:
            # Check if time field exists
            if self.time_field not in result.columns:
                # If no time field, return as is (no filtering possible)
                return result

            # Ensure time column is datetime
            if not pd.api.types.is_datetime64_any_dtype(result[self.time_field]):
                result[self.time_field] = pd.to_datetime(result[self.time_field])

            # Apply earliest filter (lower bound)
            if self.earliest is not None:
                earliest_time = self._parse_time_spec(self.earliest)
                result = result[result[self.time_field] >= earliest_time]

            # Apply latest filter
            # For relative times (negative), latest=-5m means "within last 5 minutes"
            # which sets the lower bound to now-5m
            # For absolute times, it's the upper bound
            if self.latest is not None:
                latest_time = self._parse_time_spec(self.latest)
                # Check if this is a relative past time (negative offset)
                if self.latest.startswith("-"):
                    # "latest=-5m" means data from last 5 minutes
                    result = result[result[self.time_field] >= latest_time]
                else:
                    # Absolute time or positive offset - use as upper bound
                    result = result[result[self.time_field] <= latest_time]

        return result.reset_index(drop=True)
