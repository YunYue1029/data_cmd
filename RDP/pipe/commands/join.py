"""
Join command - Join with a subquery result.

Performs left join by default.
"""

from typing import TYPE_CHECKING, Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap

if TYPE_CHECKING:
    from RDP.syntax_tree.nodes import PipeCommandNode, CommandAST


@PipeMap.register
class JoinCommand(PipeCommand):
    """
    Join command for combining DataFrames.

    Usage:
        join customer_id [search index="customers" | stats values(segment) as segment by customer_id]
    """

    keywords = ["join"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.join_field: str = kwargs.get("join_field", "")
        self.subquery_ast: "CommandAST | None" = kwargs.get("subquery_ast")
        self._executor = None  # Will be set by executor

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse join field and subquery from AST node."""
        if not self._ast_node:
            return

        from RDP.syntax_tree.nodes import PositionalArgumentNode, IdentifierNode

        # Get join field from arguments
        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, IdentifierNode):
                    self.join_field = arg.value.name

        # Get subquery
        if self._ast_node.subqueries:
            self.subquery_ast = self._ast_node.subqueries[0].command

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list (legacy)."""
        if args:
            self.join_field = args[0]

    def set_executor(self, executor: Any) -> None:
        """Set the executor for subquery execution."""
        self._executor = executor

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the join operation."""
        if df.empty:
            return df

        if not self.join_field:
            raise ValueError("Join field not specified")

        # Execute subquery to get right DataFrame
        right_df = self._execute_subquery()

        if right_df.empty:
            return df

        # Perform left join
        result = df.merge(
            right_df,
            on=self.join_field,
            how="left",
            suffixes=("", "_right"),
        )

        return result

    def _execute_subquery(self) -> pd.DataFrame:
        """Execute the subquery and return the result."""
        if self.subquery_ast is None:
            raise ValueError("No subquery specified for join")

        # Import here to avoid circular imports
        from RDP.executors import SubqueryExecutor

        executor = SubqueryExecutor(self.subquery_ast)
        return executor.execute()

