"""
Search command - Retrieve data from an index/cache.

This is used as a source command for subqueries.
In this implementation, "index" refers to a cached DataFrame.
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.commands.cache import DataFrameCache
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class SearchCommand(PipeCommand):
    """
    Search command for retrieving data.

    This is primarily used in subqueries to fetch data from
    a named source (cache/index).

    Usage:
        search index="my_data"
        search index=my_data
    """

    keywords = ["search"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.index_name = kwargs.get("index", "")

        # Parse from args if provided
        if args:
            for arg in args:
                if arg.startswith("index="):
                    # Remove quotes if present
                    value = arg[6:]
                    if (value.startswith('"') and value.endswith('"')) or \
                       (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    self.index_name = value

    @classmethod
    def from_source_name(cls, name: str, params: dict[str, Any] | None = None) -> "SearchCommand":
        """Create SearchCommand from source specification."""
        return cls(index=name)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Retrieve DataFrame from the specified index.

        The "index" is actually a cache key in this implementation.
        """
        if self.index_name:
            cached = DataFrameCache.get(self.index_name)
            if cached is None:
                raise ValueError(f"Index/cache not found: {self.index_name}")
            return cached
        return df

