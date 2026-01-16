"""
Expression Parser for eval and stats expressions.

This module provides specialized parsing for complex expressions
used in eval and stats commands.
"""

from typing import Any

from lexer import Token, TokenType
from syntax_tree.nodes import (
    ASTNode,
    FunctionCallNode,
    BinaryOpNode,
    UnaryOpNode,
    LiteralNode,
    IdentifierNode,
)


class ExpressionParser:
    """
    Parser for mathematical and logical expressions.

    Supports:
    - Arithmetic: +, -, *, /
    - Comparison: >, <, >=, <=, ==, !=
    - Logical: and, or
    - Function calls: func(arg1, arg2, ...)
    - Parenthesized expressions: (expr)
    """

    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0

    def _current_token(self) -> Token:
        """Get current token."""
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return Token(TokenType.EOF, "", len(self.tokens))

    def _peek_token(self, offset: int = 1) -> Token:
        """Peek at token at offset."""
        peek_pos = self.pos + offset
        if peek_pos < len(self.tokens):
            return self.tokens[peek_pos]
        return Token(TokenType.EOF, "", len(self.tokens))

    def _advance(self) -> Token:
        """Advance and return current token."""
        token = self._current_token()
        self.pos += 1
        return token

    def _match(self, *types: TokenType) -> bool:
        """Check if current token matches any given type."""
        return self._current_token().type in types

    def _expect(self, token_type: TokenType) -> Token:
        """Expect specific token type."""
        token = self._current_token()
        if token.type != token_type:
            raise ValueError(f"Expected {token_type}, got {token.type}")
        return self._advance()

    def parse(self) -> ASTNode:
        """Parse the complete expression."""
        return self._parse_or_expression()

    def _parse_or_expression(self) -> ASTNode:
        """Parse OR expression: and_expr (OR and_expr)*"""
        left = self._parse_and_expression()

        while self._match(TokenType.OR):
            op = self._advance().value
            right = self._parse_and_expression()
            left = BinaryOpNode(left=left, operator=op, right=right)

        return left

    def _parse_and_expression(self) -> ASTNode:
        """Parse AND expression: comparison (AND comparison)*"""
        left = self._parse_comparison()

        while self._match(TokenType.AND):
            op = self._advance().value
            right = self._parse_comparison()
            left = BinaryOpNode(left=left, operator=op, right=right)

        return left

    def _parse_comparison(self) -> ASTNode:
        """Parse comparison: additive ((>|<|>=|<=|==|!=) additive)?"""
        left = self._parse_additive()

        if self._match(
            TokenType.GT, TokenType.LT, TokenType.GTE,
            TokenType.LTE, TokenType.EQ, TokenType.NEQ
        ):
            op = self._advance().value
            right = self._parse_additive()
            left = BinaryOpNode(left=left, operator=op, right=right)

        return left

    def _parse_additive(self) -> ASTNode:
        """Parse additive: multiplicative ((+|-) multiplicative)*"""
        left = self._parse_multiplicative()

        while self._match(TokenType.PLUS, TokenType.MINUS):
            op = self._advance().value
            right = self._parse_multiplicative()
            left = BinaryOpNode(left=left, operator=op, right=right)

        return left

    def _parse_multiplicative(self) -> ASTNode:
        """Parse multiplicative: unary ((*|/) unary)*"""
        left = self._parse_unary()

        while self._match(TokenType.STAR, TokenType.SLASH):
            op = self._advance().value
            right = self._parse_unary()
            left = BinaryOpNode(left=left, operator=op, right=right)

        return left

    def _parse_unary(self) -> ASTNode:
        """Parse unary: (-|not) unary | primary"""
        if self._match(TokenType.MINUS):
            op = self._advance().value
            operand = self._parse_unary()
            return UnaryOpNode(operator=op, operand=operand)

        return self._parse_primary()

    def _parse_primary(self) -> ASTNode:
        """Parse primary: literal | identifier | function_call | (expr)"""
        position = self._current_token().position

        # Parenthesized expression
        if self._match(TokenType.LPAREN):
            self._advance()
            expr = self._parse_or_expression()
            self._expect(TokenType.RPAREN)
            return expr

        # String literal
        if self._match(TokenType.STRING):
            token = self._advance()
            return LiteralNode(value=token.value, literal_type="string", position=position)

        # Number literal
        if self._match(TokenType.NUMBER):
            token = self._advance()
            value: int | float = float(token.value) if "." in token.value else int(token.value)
            return LiteralNode(value=value, literal_type="number", position=position)

        # Identifier or function call
        if self._match(TokenType.IDENTIFIER):
            name = self._advance().value

            # Check for function call
            if self._match(TokenType.LPAREN):
                self._advance()
                args: list[ASTNode] = []

                while not self._match(TokenType.RPAREN):
                    arg = self._parse_or_expression()
                    args.append(arg)

                    if self._match(TokenType.COMMA):
                        self._advance()

                self._expect(TokenType.RPAREN)
                return FunctionCallNode(name=name, arguments=args, position=position)

            return IdentifierNode(name=name, position=position)

        raise ValueError(f"Unexpected token: {self._current_token()}")

