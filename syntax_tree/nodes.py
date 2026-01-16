"""
AST Node definitions for the command parser.

This module defines all node types used in the abstract syntax tree
representation of parsed commands.
"""

from abc import ABC
from dataclasses import dataclass, field
from typing import Any


class ASTNode(ABC):
    """Base class for all AST nodes."""

    position: int = 0  # Position in source string

    def __init__(self, position: int = 0):
        self.position = position


@dataclass
class LiteralNode(ASTNode):
    """Represents a literal value (string, number, etc.)."""

    value: Any
    literal_type: str = "string"  # "string", "number", "boolean"
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        return f"Literal({self.value!r})"


@dataclass
class IdentifierNode(ASTNode):
    """Represents an identifier (field name, command name, etc.)."""

    name: str
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        return f"Identifier({self.name})"


@dataclass
class ExpressionNode(ASTNode):
    """Base class for expression nodes."""

    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)


@dataclass
class BinaryOpNode(ASTNode):
    """Represents a binary operation (e.g., a + b, x > y)."""

    left: ASTNode
    operator: str
    right: ASTNode
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        return f"BinaryOp({self.left} {self.operator} {self.right})"


@dataclass
class UnaryOpNode(ASTNode):
    """Represents a unary operation (e.g., -x, not x)."""

    operator: str
    operand: ASTNode
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        return f"UnaryOp({self.operator} {self.operand})"


@dataclass
class FunctionCallNode(ASTNode):
    """Represents a function call (e.g., count(field), sum(amount))."""

    name: str
    arguments: list[ASTNode] = field(default_factory=list)
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        args_str = ", ".join(str(arg) for arg in self.arguments)
        return f"FunctionCall({self.name}({args_str}))"


@dataclass
class ArgumentNode(ASTNode):
    """Base class for command arguments."""

    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)


@dataclass
class PositionalArgumentNode(ASTNode):
    """Represents a positional argument."""

    value: ASTNode
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        return f"PositionalArg({self.value})"


@dataclass
class KeywordArgumentNode(ASTNode):
    """Represents a keyword argument (key=value)."""

    key: str
    value: ASTNode
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        return f"KeywordArg({self.key}={self.value})"


@dataclass
class SubqueryNode(ASTNode):
    """Represents a subquery enclosed in brackets [...]."""

    command: "CommandAST"
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        return f"Subquery([{self.command}])"


@dataclass
class SourceNode(ASTNode):
    """Represents the data source part of a command."""

    source_type: str  # "cache", "search", "index", etc.
    source_name: str
    parameters: dict[str, Any] = field(default_factory=dict)
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        params_str = ", ".join(f"{k}={v!r}" for k, v in self.parameters.items())
        if params_str:
            return f"Source({self.source_type}={self.source_name}, {params_str})"
        return f"Source({self.source_type}={self.source_name})"


@dataclass
class PipeCommandNode(ASTNode):
    """Represents a single pipe command with its arguments."""

    name: str
    arguments: list[ArgumentNode] = field(default_factory=list)
    # Special fields for specific commands
    by_fields: list[str] = field(default_factory=list)  # for stats, top, etc.
    aggregations: list[FunctionCallNode] = field(default_factory=list)  # for stats
    subqueries: list[SubqueryNode] = field(default_factory=list)  # for join
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        args_str = ", ".join(str(arg) for arg in self.arguments)
        extras = []
        if self.by_fields:
            extras.append(f"by={self.by_fields}")
        if self.aggregations:
            extras.append(f"aggs={self.aggregations}")
        if self.subqueries:
            extras.append(f"subqueries={len(self.subqueries)}")
        extra_str = ", ".join(extras)
        if extra_str:
            return f"PipeCommand({self.name}, [{args_str}], {extra_str})"
        return f"PipeCommand({self.name}, [{args_str}])"


@dataclass
class CommandAST(ASTNode):
    """
    Represents the complete AST of a command string.

    Structure:
        source | pipe_command_1 | pipe_command_2 | ...
    """

    source: SourceNode | None = None
    pipe_chain: list[PipeCommandNode] = field(default_factory=list)
    position: int = 0

    def __post_init__(self) -> None:
        super().__init__(self.position)

    def __repr__(self) -> str:
        parts = []
        if self.source:
            parts.append(str(self.source))
        for cmd in self.pipe_chain:
            parts.append(str(cmd))
        return " | ".join(parts)
