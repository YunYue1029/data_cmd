"""
AST Transformer for converting AST nodes to executable commands.

This module provides utilities for transforming the parsed AST
into PipeCommand instances that can be executed.
"""

from typing import Any

from RDP.syntax_tree.nodes import (
    ASTNode,
    CommandAST,
    PipeCommandNode,
    ArgumentNode,
    KeywordArgumentNode,
    PositionalArgumentNode,
    FunctionCallNode,
    LiteralNode,
    IdentifierNode,
    BinaryOpNode,
    UnaryOpNode,
    SubqueryNode,
)  # noqa: F401


class ASTTransformer:
    """
    Transforms AST nodes into executable format.

    Provides methods to convert AST to:
    - args list (for backward compatibility)
    - structured parameters (for new implementations)
    """

    def transform_to_args(self, node: PipeCommandNode) -> list[str]:
        """
        Transform a PipeCommandNode to an args list (backward compatible).

        Args:
            node: The PipeCommandNode to transform

        Returns:
            List of argument strings
        """
        args: list[str] = []

        # Process regular arguments
        for arg in node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                args.append(self._value_to_string(arg.value))
            elif isinstance(arg, KeywordArgumentNode):
                key = arg.key
                value = self._value_to_string(arg.value)
                args.append(f"{key}={value}")

        # Process by fields
        if node.by_fields:
            args.append("by")
            args.extend(node.by_fields)

        return args

    def transform_to_structured(self, node: PipeCommandNode) -> dict[str, Any]:
        """
        Transform a PipeCommandNode to structured parameters.

        Args:
            node: The PipeCommandNode to transform

        Returns:
            Dictionary of structured parameters
        """
        result: dict[str, Any] = {
            "command": node.name,
            "positional_args": [],
            "keyword_args": {},
            "by_fields": node.by_fields,
            "aggregations": [],
            "subqueries": [],
        }

        for arg in node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                result["positional_args"].append(self._value_to_python(arg.value))
            elif isinstance(arg, KeywordArgumentNode):
                result["keyword_args"][arg.key] = self._value_to_python(arg.value)

        for agg in node.aggregations:
            result["aggregations"].append(self._transform_function_call(agg))

        for subquery in node.subqueries:
            result["subqueries"].append(self.transform_command_ast(subquery.command))

        return result

    def transform_command_ast(self, ast: CommandAST) -> dict[str, Any]:
        """
        Transform a complete CommandAST to structured format.

        Args:
            ast: The CommandAST to transform

        Returns:
            Dictionary representing the command structure
        """
        result: dict[str, Any] = {
            "source": None,
            "pipe_chain": [],
        }

        if ast.source:
            result["source"] = {
                "type": ast.source.source_type,
                "name": ast.source.source_name,
                "parameters": ast.source.parameters,
            }

        for cmd in ast.pipe_chain:
            result["pipe_chain"].append(self.transform_to_structured(cmd))

        return result

    def _transform_function_call(self, node: FunctionCallNode) -> dict[str, Any]:
        """Transform a FunctionCallNode to dict representation."""
        return {
            "name": node.name,
            "arguments": [self._value_to_python(arg) for arg in node.arguments],
        }

    def _value_to_string(self, node: ASTNode, quote_strings: bool = False) -> str:
        """
        Convert an AST node value to string representation.

        Args:
            node: The AST node to convert
            quote_strings: Whether to add quotes around string literals

        Returns:
            String representation of the node
        """
        if isinstance(node, LiteralNode):
            if quote_strings and node.literal_type == "string":
                return f'"{node.value}"'
            return str(node.value)
        elif isinstance(node, IdentifierNode):
            return node.name
        elif isinstance(node, FunctionCallNode):
            args_str = ", ".join(self._value_to_string(arg) for arg in node.arguments)
            return f"{node.name}({args_str})"
        elif isinstance(node, BinaryOpNode):
            left = self._value_to_string(node.left)
            right = self._value_to_string(node.right)
            return f"{left} {node.operator} {right}"
        elif isinstance(node, UnaryOpNode):
            operand = self._value_to_string(node.operand)
            return f"{node.operator}{operand}"
        else:
            return str(node)

    def _value_to_python(self, node: ASTNode) -> Any:
        """Convert an AST node value to Python object."""
        if isinstance(node, LiteralNode):
            return node.value
        elif isinstance(node, IdentifierNode):
            return {"type": "identifier", "name": node.name}
        elif isinstance(node, FunctionCallNode):
            return {
                "type": "function",
                "name": node.name,
                "arguments": [self._value_to_python(arg) for arg in node.arguments],
            }
        elif isinstance(node, BinaryOpNode):
            return {
                "type": "binary_op",
                "operator": node.operator,
                "left": self._value_to_python(node.left),
                "right": self._value_to_python(node.right),
            }
        elif isinstance(node, UnaryOpNode):
            return {
                "type": "unary_op",
                "operator": node.operator,
                "operand": self._value_to_python(node.operand),
            }
        elif isinstance(node, SubqueryNode):
            return {
                "type": "subquery",
                "command": self.transform_command_ast(node.command),
            }
        else:
            return str(node)

