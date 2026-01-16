"""
Pipe Services - Factory and chain execution for commands.

This module provides:
- PipeCommandFactory: Creates command instances from parsed AST
- PipeCommandChain: Executes a chain of commands
"""

from typing import TYPE_CHECKING, Any

import pandas as pd

from pipe.pipe_map import PipeMap

if TYPE_CHECKING:
    from pipe.commands.base import PipeCommand
    from syntax_tree.nodes import PipeCommandNode


class PipeCommandFactory:
    """
    Factory for creating PipeCommand instances.

    Creates command instances from either:
    - Raw command strings (legacy)
    - Parsed PipeCommandNode AST nodes
    """

    @classmethod
    def create_from_node(cls, node: "PipeCommandNode") -> "PipeCommand":
        """
        Create a command instance from an AST node.

        Args:
            node: The PipeCommandNode containing command info

        Returns:
            A PipeCommand instance

        Raises:
            ValueError: If the command is not found
        """
        command_class = PipeMap.get(node.name)
        if command_class is None:
            raise ValueError(f"Unknown command: {node.name}")

        return command_class.from_ast_node(node)

    @classmethod
    def create(cls, raw: str) -> "PipeCommand":
        """
        Create a command instance from a raw command string (legacy).

        Args:
            raw: The raw command string (e.g., "stats count by field")

        Returns:
            A PipeCommand instance
        """
        parts = raw.strip().split(None, 1)  # Split into command and args
        cmd_name = parts[0] if parts else ""
        args_str = parts[1] if len(parts) > 1 else ""

        command_class = PipeMap.get(cmd_name)
        if command_class is None:
            raise ValueError(f"Unknown command: {cmd_name}")

        # Parse args string into list
        args = cls._parse_args_string(args_str) if args_str else []
        return command_class(args)

    @staticmethod
    def _parse_args_string(args_str: str) -> list[str]:
        """
        Parse arguments string into list, handling quotes.

        Args:
            args_str: The arguments string

        Returns:
            List of argument strings
        """
        args: list[str] = []
        current: list[str] = []
        in_quotes = False
        quote_char = None
        i = 0

        while i < len(args_str):
            char = args_str[i]

            if in_quotes:
                if char == quote_char:
                    in_quotes = False
                    current.append(char)
                elif char == "\\":
                    # Escape sequence
                    if i + 1 < len(args_str):
                        current.append(char)
                        i += 1
                        current.append(args_str[i])
                else:
                    current.append(char)
            elif char in "\"'":
                in_quotes = True
                quote_char = char
                current.append(char)
            elif char in " \t":
                if current:
                    args.append("".join(current))
                    current = []
            else:
                current.append(char)

            i += 1

        if current:
            args.append("".join(current))

        return args


class PipeCommandChain:
    """
    Chain of commands to be executed sequentially.

    Each command receives the output DataFrame of the previous command.
    """

    def __init__(self, commands: list["PipeCommand"] | None = None):
        self._commands: list["PipeCommand"] = commands or []

    def add(self, command: "PipeCommand") -> "PipeCommandChain":
        """
        Add a command to the chain.

        Args:
            command: The command to add

        Returns:
            Self for method chaining
        """
        self._commands.append(command)
        return self

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute all commands in the chain.

        Args:
            df: The input DataFrame

        Returns:
            The output DataFrame after all commands
        """
        result = df
        for command in self._commands:
            result = command.execute(result)
        return result

    def __len__(self) -> int:
        return len(self._commands)

    def __iter__(self):
        return iter(self._commands)

