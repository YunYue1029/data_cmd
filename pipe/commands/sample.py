"""
Sample command - Randomly sample rows from DataFrame.
"""

from typing import Any

import pandas as pd

from pipe.commands.base import PipeCommand
from pipe.pipe_map import PipeMap


@PipeMap.register
class SampleCommand(PipeCommand):
    """
    Sample command for random sampling.

    Usage:
        sample 100           # Sample 100 rows
        sample 100 seed=42   # Sample with fixed seed for reproducibility
        sample ratio=0.1     # Sample 10% of rows
        sample ratio=0.5 seed=42
    """

    keywords = ["sample", "rand"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.n: int | None = kwargs.get("n")
        self.ratio: float | None = kwargs.get("ratio")
        self.seed: int | None = kwargs.get("seed")

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

        # Default to 10 if nothing specified
        if self.n is None and self.ratio is None:
            self.n = 10

    def _parse_from_ast(self) -> None:
        """Parse sample parameters from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import (
            PositionalArgumentNode,
            KeywordArgumentNode,
            LiteralNode,
        )

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    self.n = int(arg.value.value)
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    value = arg.value.value
                    if arg.key == "n":
                        self.n = int(value)
                    elif arg.key == "ratio":
                        self.ratio = float(value)
                    elif arg.key == "seed":
                        self.seed = int(value)

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("ratio="):
                self.ratio = float(arg[6:])
            elif arg.startswith("seed="):
                self.seed = int(arg[5:])
            elif arg.startswith("n="):
                self.n = int(arg[2:])
            else:
                try:
                    self.n = int(arg)
                except ValueError:
                    pass

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the sample operation."""
        if df.empty:
            return df

        if self.ratio is not None:
            # Sample by fraction
            frac = min(1.0, max(0.0, self.ratio))
            return df.sample(frac=frac, random_state=self.seed).reset_index(drop=True)
        else:
            # Sample by count
            n = min(self.n or 10, len(df))
            return df.sample(n=n, random_state=self.seed).reset_index(drop=True)

