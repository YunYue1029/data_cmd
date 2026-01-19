"""
Replace command - Replace values in DataFrame columns.
"""

import re
from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class ReplaceCommand(PipeCommand):
    """
    Replace command for replacing values in columns.

    Usage:
        replace field old_value with new_value
        replace field "old text" with "new text"
        replace field regex="pattern" with "replacement"
        replace field null with "N/A"
    """

    keywords = ["replace"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.replacements: list[dict[str, Any]] = kwargs.get("replacements", [])

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse replacements from AST node."""
        if not self._ast_node:
            return

        from syntax_tree.nodes import (
            PositionalArgumentNode,
            KeywordArgumentNode,
            LiteralNode,
            IdentifierNode,
        )

        field = None
        old_value = None
        new_value = None
        is_regex = False

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                if isinstance(arg.value, IdentifierNode):
                    if field is None:
                        field = arg.value.name
                    elif old_value is None:
                        old_value = arg.value.name
                    else:
                        new_value = arg.value.name
                elif isinstance(arg.value, LiteralNode):
                    val = arg.value.value
                    if old_value is None:
                        old_value = val
                    else:
                        new_value = val
            elif isinstance(arg, KeywordArgumentNode):
                if arg.key == "regex":
                    is_regex = True
                    if isinstance(arg.value, LiteralNode):
                        old_value = str(arg.value.value)
                elif arg.key == "with":
                    if isinstance(arg.value, LiteralNode):
                        new_value = arg.value.value

        if field and old_value is not None:
            self.replacements.append({
                "field": field,
                "old_value": old_value,
                "new_value": new_value,
                "is_regex": is_regex,
            })

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        if len(args) < 3:
            return

        i = 0
        while i < len(args):
            if i + 3 > len(args):
                break

            field = args[i]
            old_value = args[i + 1]
            is_regex = False

            # Check for "regex=" prefix
            if old_value.startswith("regex="):
                is_regex = True
                old_value = old_value[6:]

            # Remove quotes
            old_value = old_value.strip('"\'')

            # Skip "with" keyword
            if i + 2 < len(args) and args[i + 2].lower() == "with":
                if i + 3 < len(args):
                    new_value = args[i + 3].strip('"\'')
                    i += 4
                else:
                    break
            else:
                new_value = args[i + 2].strip('"\'')
                i += 3

            # Handle null keyword
            if old_value.lower() == "null":
                old_value = None

            self.replacements.append({
                "field": field,
                "old_value": old_value,
                "new_value": new_value,
                "is_regex": is_regex,
            })

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the replace operation."""
        if df.empty or not self.replacements:
            return df

        result = df.copy()

        for repl in self.replacements:
            field = repl["field"]
            old_value = repl["old_value"]
            new_value = repl["new_value"]
            is_regex = repl["is_regex"]

            if field not in result.columns:
                raise ValueError(f"Field not found: {field}")

            if old_value is None:
                # Replace null values
                result[field] = result[field].fillna(new_value)
            elif is_regex:
                # Regex replacement
                result[field] = result[field].astype(str).str.replace(
                    old_value, new_value, regex=True
                )
            else:
                # Exact value replacement
                result[field] = result[field].replace(old_value, new_value)

        return result

