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

        from RDP.syntax_tree.nodes import (
            PositionalArgumentNode,
            KeywordArgumentNode,
            LiteralNode,
            IdentifierNode,
        )

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    val = str(arg.value.value)
                    # Check for sed-style s/pattern/replacement/ syntax
                    if val.startswith("s/") and "/" in val[2:]:
                        self._parse_sed_syntax(val)
                    elif not self.pattern:
                        self.pattern = val
            elif isinstance(arg, KeywordArgumentNode):
                # Handle both LiteralNode and IdentifierNode values
                if isinstance(arg.value, LiteralNode):
                    val = str(arg.value.value)
                elif isinstance(arg.value, IdentifierNode):
                    val = arg.value.name
                else:
                    val = str(arg.value)
                
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

    def _parse_sed_syntax(self, sed_expr: str) -> None:
        """
        Parse sed-style s/pattern/replacement/ syntax.
        
        Args:
            sed_expr: Expression like "s/Hello/Hi/" or "s/pattern/replacement/g"
        """
        # Remove the leading 's/'
        expr = sed_expr[2:]
        
        # Find the delimiter (first character after s/)
        # Handle escaped delimiters
        parts = []
        current = []
        i = 0
        while i < len(expr):
            char = expr[i]
            if char == "\\":
                # Escaped character - include it
                if i + 1 < len(expr):
                    current.append(char)
                    current.append(expr[i + 1])
                    i += 2
                    continue
            elif char == "/":
                parts.append("".join(current))
                current = []
            else:
                current.append(char)
            i += 1
        
        if current:
            parts.append("".join(current))
        
        if len(parts) >= 2:
            self.pattern = parts[0]
            self.replacement = parts[1]
            self.mode = "sed"

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
            elif arg.startswith("s/") and "/" in arg[2:]:
                # sed-style s/pattern/replacement/ syntax
                self._parse_sed_syntax(arg)
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
            # Convert Perl-style (?<name>) to Python-style (?P<name>)
            pattern = re.sub(r'\(\?<(\w+)>', r'(?P<\1>', self.pattern)
            
            try:
                compiled = re.compile(pattern)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {e}")

            # Get named groups
            group_names = list(compiled.groupindex.keys())

            if not group_names:
                # No named groups - try to extract to a default field
                extracted = result[self.field].astype(str).str.extract(pattern)
                for i, col in enumerate(extracted.columns):
                    result[f"extract_{i+1}"] = extracted[col]
            else:
                # Extract named groups
                extracted = result[self.field].astype(str).str.extract(pattern)

                # Rename columns to match named groups
                for i, name in enumerate(group_names):
                    if i < len(extracted.columns):
                        result[name] = extracted.iloc[:, i]

        return result

