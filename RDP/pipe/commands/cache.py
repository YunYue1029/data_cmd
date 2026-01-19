"""
Cache command - Store and retrieve DataFrames from cache.

This module provides:
- Global cache storage for DataFrames
- CacheCommand for retrieving cached data
- NewCacheCommand for storing data to cache
"""

from typing import Any, ClassVar

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


class DataFrameCache:
    """
    Global cache storage for DataFrames.

    This is a simple in-memory cache that allows storing and
    retrieving DataFrames by name.
    """

    _cache: ClassVar[dict[str, pd.DataFrame]] = {}

    @classmethod
    def set(cls, name: str, df: pd.DataFrame) -> None:
        """
        Store a DataFrame in the cache.

        Args:
            name: The cache key
            df: The DataFrame to store
        """
        cls._cache[name] = df

    @classmethod
    def get(cls, name: str) -> pd.DataFrame | None:
        """
        Retrieve a DataFrame from the cache.

        Args:
            name: The cache key

        Returns:
            The cached DataFrame or None if not found
        """
        return cls._cache.get(name)

    @classmethod
    def has(cls, name: str) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            name: The cache key

        Returns:
            True if the key exists
        """
        return name in cls._cache

    @classmethod
    def delete(cls, name: str) -> bool:
        """
        Delete a key from the cache.

        Args:
            name: The cache key

        Returns:
            True if the key was deleted
        """
        if name in cls._cache:
            del cls._cache[name]
            return True
        return False

    @classmethod
    def clear(cls) -> None:
        """Clear all cached DataFrames."""
        cls._cache.clear()

    @classmethod
    def list(cls) -> list[str]:
        """List all cache keys."""
        return list(cls._cache.keys())


@PipeMap.register
class CacheCommand(PipeCommand):
    """
    Command to retrieve data from cache.

    This is used as a source command, not a pipe command.

    Usage (as source):
        cache=my_data | stats count by field
    """

    keywords = ["cache"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.cache_name = kwargs.get("name", "")

        # Parse from args if provided
        if args:
            for arg in args:
                if arg.startswith("name="):
                    self.cache_name = arg[5:]
                elif "=" not in arg:
                    self.cache_name = arg

    @classmethod
    def from_source_name(cls, name: str) -> "CacheCommand":
        """Create CacheCommand from source name."""
        return cls(name=name)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Retrieve DataFrame from cache.

        If cache_name is set, returns the cached DataFrame.
        Otherwise, passes through the input DataFrame.
        """
        if self.cache_name:
            cached = DataFrameCache.get(self.cache_name)
            if cached is None:
                raise ValueError(f"Cache key not found: {self.cache_name}")
            return cached
        return df


@PipeMap.register
class NewCacheCommand(PipeCommand):
    """
    Command to store data to cache.

    Usage:
        source | new_cache name=my_data
    """

    keywords = ["new_cache", "newcache", "tocache"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.cache_name = kwargs.get("name", "")

        # Parse from args if provided
        if args:
            for arg in args:
                if arg.startswith("name="):
                    self.cache_name = arg[5:]
                elif "=" not in arg:
                    self.cache_name = arg

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Store DataFrame to cache and return it.

        Args:
            df: The DataFrame to cache

        Returns:
            The same DataFrame (pass-through)
        """
        if self.cache_name:
            DataFrameCache.set(self.cache_name, df)
        return df

