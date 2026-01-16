"""
Command Executors - Entry point for executing commands.

This module provides the main CommandExecutor class that users
interact with to execute command strings.
"""

from typing import Any

import pandas as pd

from parser.command_parser import CommandParser
from syntax_tree.nodes import CommandAST
from planner.query_planner import QueryPlanner
from pipe.services import PipeCommandChain
from pipe.commands.cache import DataFrameCache

# Import commands module to trigger registration
from pipe import commands as _  # noqa: F401


class CommandExecutor:
    """
    Main executor for command strings.

    This is the primary interface for executing command pipelines.

    Usage:
        executor = CommandExecutor("cache=my_data | stats count by field")
        result = executor.execute()
    """

    def __init__(self, cmd: str, **context: Any):
        """
        Initialize the executor.

        Args:
            cmd: The command string to execute
            **context: Additional context (start_time, end_time, etc.)
        """
        self.cmd = cmd
        self.context = context
        self._ast: CommandAST | None = None
        self._planner = QueryPlanner()

    def parse(self) -> CommandAST:
        """
        Parse the command string into an AST.

        Returns:
            The parsed CommandAST
        """
        if self._ast is None:
            parser = CommandParser(self.cmd)
            self._ast = parser.parse()
        return self._ast

    def execute(self) -> pd.DataFrame:
        """
        Execute the command and return the result.

        Returns:
            The resulting DataFrame
        """
        # Parse command
        ast = self.parse()

        # Create and optimize execution plan
        plan = self._planner.create_plan(ast)
        plan = self._planner.optimize(plan)

        # Get source DataFrame
        df = self._get_source_data(plan.source_type, plan.source_name, plan.source_params)

        # Create and execute command chain
        commands = self._planner.create_commands(plan)
        chain = PipeCommandChain(commands)

        return chain.execute(df)

    def _get_source_data(
        self,
        source_type: str,
        source_name: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        """
        Get the source DataFrame based on source specification.

        Args:
            source_type: Type of source (cache, search, etc.)
            source_name: Name of the source
            params: Additional parameters

        Returns:
            The source DataFrame
        """
        if source_type == "cache":
            df = DataFrameCache.get(source_name)
            if df is None:
                raise ValueError(f"Cache not found: {source_name}")
            return df

        elif source_type == "search":
            # Search uses cache as the index source
            df = DataFrameCache.get(source_name)
            if df is None:
                raise ValueError(f"Index not found: {source_name}")
            return df

        elif source_type == "default":
            # Try to find in cache
            df = DataFrameCache.get(source_name)
            if df is not None:
                return df
            raise ValueError(f"Source not found: {source_name}")

        else:
            raise ValueError(f"Unknown source type: {source_type}")


class SubqueryExecutor:
    """
    Executor for subqueries.

    Used internally by commands like join to execute subquery ASTs.
    """

    def __init__(self, ast: CommandAST):
        """
        Initialize the subquery executor.

        Args:
            ast: The parsed subquery AST
        """
        self.ast = ast
        self._planner = QueryPlanner()

    def execute(self) -> pd.DataFrame:
        """
        Execute the subquery and return the result.

        Returns:
            The resulting DataFrame
        """
        # Create and optimize execution plan
        plan = self._planner.create_plan(self.ast)
        plan = self._planner.optimize(plan)

        # Get source DataFrame
        df = self._get_source_data(plan.source_type, plan.source_name, plan.source_params)

        # Create and execute command chain
        commands = self._planner.create_commands(plan)
        chain = PipeCommandChain(commands)

        return chain.execute(df)

    def _get_source_data(
        self,
        source_type: str,
        source_name: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        """Get the source DataFrame."""
        if source_type in ("cache", "search", "default"):
            df = DataFrameCache.get(source_name)
            if df is None:
                raise ValueError(f"Source not found: {source_name}")
            return df
        else:
            raise ValueError(f"Unknown source type: {source_type}")


# Convenience function to register a DataFrame to cache
def register_cache(name: str, df: pd.DataFrame) -> None:
    """
    Register a DataFrame to the global cache.

    Args:
        name: The cache key
        df: The DataFrame to cache
    """
    DataFrameCache.set(name, df)


def clear_cache() -> None:
    """Clear all cached DataFrames."""
    DataFrameCache.clear()


def list_cache() -> list[str]:
    """List all cache keys."""
    return DataFrameCache.list()

