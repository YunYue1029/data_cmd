"""
Syntax Tree module for command parsing.

This module defines the node types that represent parsed commands
and provides transformation utilities.

Note: Named 'syntax_tree' instead of 'ast' to avoid conflict with Python's built-in ast module.
"""

from syntax_tree.nodes import (
    ASTNode,
    CommandAST,
    PipeCommandNode,
    SourceNode,
    ArgumentNode,
    KeywordArgumentNode,
    PositionalArgumentNode,
    FunctionCallNode,
    SubqueryNode,
    ExpressionNode,
    BinaryOpNode,
    UnaryOpNode,
    LiteralNode,
    IdentifierNode,
)
from syntax_tree.transformer import ASTTransformer

__all__ = [
    "ASTNode",
    "CommandAST",
    "PipeCommandNode",
    "SourceNode",
    "ArgumentNode",
    "KeywordArgumentNode",
    "PositionalArgumentNode",
    "FunctionCallNode",
    "SubqueryNode",
    "ExpressionNode",
    "BinaryOpNode",
    "UnaryOpNode",
    "LiteralNode",
    "IdentifierNode",
    "ASTTransformer",
]

