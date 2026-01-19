"""
Rex command - Extract fields using regular expressions.
"""

import re
from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class RexCommand(PipeCommand):
    """
    Rex command for regex field extraction.

    Uses named capture groups (?P<name>...) to extract fields.

    Usage:
        rex field="message" "(?P<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)"
        rex field="log" "user=(?P<user>\\w+)"
        rex field="url" "/(?P<path>[^?]+)\\?(?P<query>.*)"
        rex field="text" pattern="(?P<num>\\d+)" mode=sed replacement="[NUMBER]"
    """

    keywords = ["rex", "regex", "extract"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.field: str = kwargs.get("field", "")
        self.pattern: str = kwargs.get("pattern", "")
        self.mode: str = kwargs.get("mode", "extract")  # 'extract' or 'sed'
        self.replacement: str = kwargs.get("replacement", "")
        self.max_match: int = kwargs.get("max_match", 1)  # Number of matches to extract

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

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
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    # Positional argument is the pattern
                    if not self.pattern:
                        self.pattern = str(arg.value.value)
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    val = str(arg.value.value)
                    if arg.key == "field":
                        self.field = val
                    elif arg.key == "pattern":
                        self.pattern = val
                    elif arg.key == "mode":
                        self.mode = val.lower()
                    elif arg.key == "replacement":
                        self.replacement = val
                    elif arg.key == "max_match":
                        self.max_match = int(val)

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("field="):
                self.field = arg[6:].strip('"\'')
            elif arg.startswith("pattern="):
                self.pattern = arg[8:].strip('"\'')
            elif arg.startswith("mode="):
                self.mode = arg[5:].lower()
            elif arg.startswith("replacement="):
                self.replacement = arg[12:].strip('"\'')
            elif arg.startswith("max_match="):
                self.max_match = int(arg[10:])
            elif not self.pattern and (arg.startswith('"') or arg.startswith("'")):
                self.pattern = arg.strip('"\'')
            elif not self.pattern:
                self.pattern = arg

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the rex operation."""
        if df.empty or not self.pattern:
            return df

        if not self.field:
            raise ValueError("Field not specified for rex command")

        if self.field not in df.columns:
            raise ValueError(f"Field not found: {self.field}")

        result = df.copy()

        if self.mode == "sed":
            # Replacement mode
            result[self.field] = result[self.field].astype(str).str.replace(
                self.pattern, self.replacement, regex=True
            )
        else:
            # Extract mode - use named capture groups
            try:
                compiled = re.compile(self.pattern)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {e}")

            # Get named groups
            group_names = list(compiled.groupindex.keys())

            if not group_names:
                # No named groups - try to extract to a default field
                extracted = result[self.field].astype(str).str.extract(self.pattern)
                for i, col in enumerate(extracted.columns):
                    result[f"extract_{i+1}"] = extracted[col]
            else:
                # Extract named groups
                extracted = result[self.field].astype(str).str.extract(self.pattern)

                # Rename columns to match named groups
                for i, name in enumerate(group_names):
                    if i < len(extracted.columns):
                        result[name] = extracted.iloc[:, i]

        return result

