"""
Head command - Return the first N rows.
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class HeadCommand(PipeCommand):
    """
    Head command for limiting rows.

    Usage:
        head 10      # First 10 rows
        head         # First 10 rows (default)
    """

    keywords = ["head", "limit"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.limit: int = kwargs.get("limit", 10)

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse limit from AST node."""
        if not self._ast_node:
            return

        from RDP.syntax_tree.nodes import PositionalArgumentNode, LiteralNode

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    self.limit = int(arg.value.value)

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        if args:
            try:
                self.limit = int(args[0])
            except ValueError:
                self.limit = 10

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return the first N rows."""
        return df.head(self.limit)

