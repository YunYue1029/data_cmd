"""
Base class for all pipe commands.

This module defines the abstract base class that all
pipe commands must inherit from.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from RDP.syntax_tree.nodes import PipeCommandNode


class PipeCommand(ABC):
    """
    Abstract base class for pipe commands.

    All pipe commands must implement:
    - execute(df): Process the DataFrame and return the result
    - keywords: List of command keywords (class attribute)

    Optional overrides:
    - from_ast_node(): Create instance from AST node
    - validate(): Validate command arguments
    """

    # Command keywords (subclasses should override)
    keywords: list[str] = []

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        """
        Initialize the command.

        Args:
            args: List of argument strings (legacy format)
            **kwargs: Additional keyword arguments (including _ast_node)
        """
        self.args = args or []
        self._ast_node: "PipeCommandNode | None" = kwargs.pop("_ast_node", None)
        self.kwargs = kwargs

    @classmethod
    def from_ast_node(cls, node: "PipeCommandNode") -> "PipeCommand":
        """
        Create a command instance from an AST node.

        Subclasses can override this for custom AST handling.

        Args:
            node: The PipeCommandNode containing command info

        Returns:
            A new command instance
        """
        # Convert AST node to args list (default behavior)
        args = cls._node_to_args(node)
        # Pass the AST node to __init__ so it's available during initialization
        instance = cls(args, _ast_node=node)
        return instance

    @classmethod
    def _node_to_args(cls, node: "PipeCommandNode") -> list[str]:
        """
        Convert AST node to args list (for backward compatibility).

        Args:
            node: The PipeCommandNode

        Returns:
            List of argument strings
        """
        from RDP.syntax_tree.transformer import ASTTransformer
        transformer = ASTTransformer()
        return transformer.transform_to_args(node)

    @abstractmethod
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the command on the DataFrame.

        Args:
            df: The input DataFrame

        Returns:
            The processed DataFrame
        """
        pass

    def validate(self, df: pd.DataFrame) -> None:
        """
        Validate the command before execution.

        Args:
            df: The input DataFrame

        Raises:
            ValueError: If validation fails
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.args})"

