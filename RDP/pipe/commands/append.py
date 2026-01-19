"""
Append command - Append results from a subquery to current results.

Used to combine data from multiple sources into a single result set.
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class AppendCommand(PipeCommand):
    """
    Append command for combining results from subqueries.

    Appends rows from a subquery to the current result set.
    Columns are aligned, with missing columns filled with NaN.

    Usage:
        cache=data1 | append [search index="data2"]
        cache=logs | append [search index="errors"] | stats count by type
    """

    keywords = ["append", "union"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.subquery_ast = None

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()

    def _parse_from_ast(self) -> None:
        """Parse from AST node."""
        if not self._ast_node:
            return

        # Get subquery from the node's subqueries list
        if self._ast_node.subqueries:
            self.subquery_ast = self._ast_node.subqueries[0]

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the append operation.

        Appends rows from subquery result to the input DataFrame.
        """
        if self.subquery_ast is None:
            # No subquery specified, return as is
            return df

        # Execute the subquery
        # SubqueryNode contains a CommandAST in its 'command' attribute
        from RDP.executors import SubqueryExecutor
        subquery_command = self.subquery_ast.command if hasattr(self.subquery_ast, 'command') else self.subquery_ast
        subquery_result = SubqueryExecutor(subquery_command).execute()

        if subquery_result.empty:
            return df

        if df.empty:
            return subquery_result

        # Concatenate the two DataFrames
        # Align columns - include all columns from both DataFrames
        all_columns = list(df.columns) + [c for c in subquery_result.columns if c not in df.columns]

        # Reindex both DataFrames to have all columns
        df_aligned = df.reindex(columns=all_columns)
        subquery_aligned = subquery_result.reindex(columns=all_columns)

        # Concatenate
        result = pd.concat([df_aligned, subquery_aligned], ignore_index=True)

        return result

