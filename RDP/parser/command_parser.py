"""
Command Parser - Recursive descent parser for command syntax.

This module parses command strings into AST nodes, handling:
- Source specifications (cache=name, search index=name, etc.)
- Pipe commands with arguments
- Subqueries in brackets [...]
- Function calls and expressions
"""

from typing import Any

from RDP.lexer import CommandLexer, Token, TokenType, LexerError
from RDP.syntax_tree.nodes import (
    CommandAST,
    PipeCommandNode,
    SourceNode,
    ArgumentNode,
    KeywordArgumentNode,
    PositionalArgumentNode,
    FunctionCallNode,
    SubqueryNode,
    LiteralNode,
    IdentifierNode,
    BinaryOpNode,
    ASTNode,
)


class ParserError(Exception):
    """Exception raised for parser errors."""

    def __init__(self, message: str, token: Token | None = None):
        self.token = token
        if token:
            super().__init__(f"{message} at position {token.position}")
        else:
            super().__init__(message)


class CommandParser:
    """
    Recursive descent parser for command syntax.

    Grammar (simplified):
        command     := source (PIPE pipe_command)*
        source      := IDENTIFIER EQUALS value | IDENTIFIER
        pipe_command := IDENTIFIER arguments
        arguments   := argument*
        argument    := IDENTIFIER EQUALS value | value | subquery
        subquery    := LBRACKET command RBRACKET
        value       := STRING | NUMBER | IDENTIFIER | function_call
        function_call := IDENTIFIER LPAREN arg_list? RPAREN
        arg_list    := expression (COMMA expression)*
    """

    def __init__(self, source: str):
        self.source = source
        self.lexer = CommandLexer(source)
        self.tokens: list[Token] = []
        self.pos = 0

    def parse(self) -> CommandAST:
        """Parse the command string into an AST."""
        try:
            self.tokens = self.lexer.tokenize()
        except LexerError as e:
            raise ParserError(str(e))

        self.pos = 0
        return self._parse_command()

    def _current_token(self) -> Token:
        """Get the current token."""
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return self.tokens[-1]  # Return EOF token

    def _peek_token(self, offset: int = 1) -> Token:
        """Peek at token at offset from current position."""
        peek_pos = self.pos + offset
        if peek_pos < len(self.tokens):
            return self.tokens[peek_pos]
        return self.tokens[-1]

    def _advance(self) -> Token:
        """Advance and return the current token."""
        token = self._current_token()
        self.pos += 1
        return token

    def _expect(self, token_type: TokenType) -> Token:
        """Expect a specific token type, raise error if not found."""
        token = self._current_token()
        if token.type != token_type:
            raise ParserError(
                f"Expected {token_type.name}, got {token.type.name}", token
            )
        return self._advance()

    def _match(self, *token_types: TokenType) -> bool:
        """Check if current token matches any of the given types."""
        return self._current_token().type in token_types

    def _parse_command(self) -> CommandAST:
        """Parse a complete command: source | pipe1 | pipe2 | ..."""
        ast = CommandAST(position=self._current_token().position)

        # Parse source
        ast.source = self._parse_source()

        # Parse pipe chain
        while self._match(TokenType.PIPE):
            self._advance()  # consume pipe
            pipe_cmd = self._parse_pipe_command()
            ast.pipe_chain.append(pipe_cmd)

        return ast

    def _parse_source(self) -> SourceNode:
        """Parse source specification: cache=name, search index=name, etc."""
        position = self._current_token().position

        # Check for source_type=source_name pattern
        if self._match(TokenType.IDENTIFIER):
            first_ident = self._advance()

            if self._match(TokenType.EQUALS):
                # source_type=source_name (e.g., cache=test_data)
                self._advance()  # consume =
                source_name = self._parse_value_as_string()
                return SourceNode(
                    source_type=first_ident.value,
                    source_name=source_name,
                    position=position,
                )
            else:
                # Just an identifier (e.g., "search" followed by parameters)
                # This is a command like "search index=xxx"
                if first_ident.value.lower() == "search":
                    return self._parse_search_source(position)
                else:
                    # Treat as simple source name
                    return SourceNode(
                        source_type="default",
                        source_name=first_ident.value,
                        position=position,
                    )

        raise ParserError("Expected source specification", self._current_token())

    def _parse_search_source(self, position: int) -> SourceNode:
        """Parse search source: search index="name" ..."""
        params: dict[str, Any] = {}
        index_name = ""

        # Parse search parameters
        while not self._match(TokenType.PIPE, TokenType.EOF):
            if self._match(TokenType.IDENTIFIER):
                key = self._advance().value

                if self._match(TokenType.EQUALS):
                    self._advance()  # consume =
                    value = self._parse_value_as_string()
                    if key.lower() == "index":
                        index_name = value
                    else:
                        params[key] = value
                else:
                    # Positional value
                    if not index_name:
                        index_name = key
                    else:
                        params[key] = True
            else:
                break

        return SourceNode(
            source_type="search",
            source_name=index_name,
            parameters=params,
            position=position,
        )

    def _parse_pipe_command(self) -> PipeCommandNode:
        """Parse a pipe command: command_name arguments"""
        position = self._current_token().position

        # Command name
        if not self._match(TokenType.IDENTIFIER):
            raise ParserError("Expected command name", self._current_token())

        cmd_name = self._advance().value
        node = PipeCommandNode(name=cmd_name, position=position)

        # Parse arguments based on command type
        if cmd_name.lower() in ("stats", "eventstats"):
            self._parse_stats_arguments(node)
        elif cmd_name.lower() == "sort":
            self._parse_sort_arguments(node)
        elif cmd_name.lower() == "join":
            self._parse_join_arguments(node)
        elif cmd_name.lower() == "head":
            self._parse_head_arguments(node)
        elif cmd_name.lower() == "filter":
            self._parse_filter_arguments(node)
        else:
            # Generic argument parsing
            self._parse_generic_arguments(node)

        return node

    def _parse_stats_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse stats command arguments.
        Format: stats func(field) as alias, ... by field1, field2, ...
        """
        # Parse aggregation functions
        while not self._match(TokenType.PIPE, TokenType.EOF, TokenType.BY):
            if self._match(TokenType.IDENTIFIER):
                # Could be function call or 'count' without parentheses
                agg = self._parse_aggregation()
                node.aggregations.append(agg)

                # Skip comma between aggregations
                if self._match(TokenType.COMMA):
                    self._advance()
            else:
                break

        # Parse BY clause
        if self._match(TokenType.BY):
            self._advance()  # consume 'by'
            while not self._match(TokenType.PIPE, TokenType.EOF):
                if self._match(TokenType.IDENTIFIER):
                    node.by_fields.append(self._advance().value)
                    if self._match(TokenType.COMMA):
                        self._advance()
                else:
                    break

    def _parse_aggregation(self) -> FunctionCallNode:
        """Parse an aggregation function: func(field) as alias"""
        position = self._current_token().position
        name = self._advance().value  # function name

        args: list[ASTNode] = []
        alias: str | None = None

        # Check for parentheses (function call)
        if self._match(TokenType.LPAREN):
            self._advance()  # consume (

            # Parse arguments
            while not self._match(TokenType.RPAREN):
                arg = self._parse_expression()
                args.append(arg)

                if self._match(TokenType.COMMA):
                    self._advance()
                elif not self._match(TokenType.RPAREN):
                    break

            self._expect(TokenType.RPAREN)

        # Check for 'as' alias
        if self._match(TokenType.AS):
            self._advance()  # consume 'as'
            if self._match(TokenType.IDENTIFIER):
                alias = self._advance().value

        func_node = FunctionCallNode(name=name, arguments=args, position=position)

        # Store alias in the function name if provided
        if alias:
            func_node.name = f"{name}:{alias}"  # Encode alias in name

        return func_node

    def _parse_sort_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse sort command arguments.
        Format: sort [-]field1, [-]field2, ...
        """
        while not self._match(TokenType.PIPE, TokenType.EOF):
            if self._match(TokenType.MINUS):
                # Descending sort
                self._advance()
                if self._match(TokenType.IDENTIFIER):
                    field = self._advance().value
                    node.arguments.append(
                        PositionalArgumentNode(
                            value=LiteralNode(value=f"-{field}"),
                            position=self._current_token().position,
                        )
                    )
            elif self._match(TokenType.IDENTIFIER):
                field = self._advance().value
                node.arguments.append(
                    PositionalArgumentNode(
                        value=LiteralNode(value=field),
                        position=self._current_token().position,
                    )
                )

            if self._match(TokenType.COMMA):
                self._advance()
            elif not self._match(TokenType.PIPE, TokenType.EOF):
                # No comma, might be end of sort fields
                if not self._match(TokenType.MINUS, TokenType.IDENTIFIER):
                    break

    def _parse_join_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse join command arguments.
        Format: join field [subquery]
        """
        # Parse join field
        if self._match(TokenType.IDENTIFIER):
            join_field = self._advance().value
            node.arguments.append(
                PositionalArgumentNode(
                    value=IdentifierNode(name=join_field),
                    position=self._current_token().position,
                )
            )

        # Parse subquery
        if self._match(TokenType.LBRACKET):
            subquery = self._parse_subquery()
            node.subqueries.append(subquery)

    def _parse_subquery(self) -> SubqueryNode:
        """Parse a subquery: [command]"""
        position = self._current_token().position
        self._expect(TokenType.LBRACKET)

        # Parse the inner command
        inner_ast = self._parse_command()

        self._expect(TokenType.RBRACKET)

        return SubqueryNode(command=inner_ast, position=position)

    def _parse_head_arguments(self, node: PipeCommandNode) -> None:
        """Parse head command arguments: head N"""
        if self._match(TokenType.NUMBER):
            limit = self._advance().value
            node.arguments.append(
                PositionalArgumentNode(
                    value=LiteralNode(value=int(limit), literal_type="number"),
                    position=self._current_token().position,
                )
            )

    def _parse_filter_arguments(self, node: PipeCommandNode) -> None:
        """Parse filter command arguments: filter field=value field2=value2 ..."""
        while not self._match(TokenType.PIPE, TokenType.EOF):
            if self._match(TokenType.IDENTIFIER):
                field = self._advance().value

                if self._match(TokenType.EQUALS, TokenType.EQ, TokenType.GT, TokenType.LT, TokenType.GTE, TokenType.LTE, TokenType.NEQ):
                    op_token = self._advance()
                    value = self._parse_value()
                    node.arguments.append(
                        KeywordArgumentNode(
                            key=field,
                            value=BinaryOpNode(
                                left=IdentifierNode(name=field),
                                operator=op_token.value,
                                right=value,
                                position=self._current_token().position,
                            ),
                            position=self._current_token().position,
                        )
                    )
                else:
                    # Just field name (boolean true condition)
                    node.arguments.append(
                        PositionalArgumentNode(
                            value=IdentifierNode(name=field),
                            position=self._current_token().position,
                        )
                    )
            else:
                break

    def _parse_generic_arguments(self, node: PipeCommandNode) -> None:
        """Parse generic command arguments."""
        while not self._match(TokenType.PIPE, TokenType.EOF):
            # Check for keyword argument
            if self._match(TokenType.IDENTIFIER):
                if self._peek_token().type == TokenType.EQUALS:
                    key = self._advance().value
                    self._advance()  # consume =
                    value = self._parse_value()
                    node.arguments.append(
                        KeywordArgumentNode(key=key, value=value, position=self._current_token().position)
                    )
                    continue

            # Check for subquery
            if self._match(TokenType.LBRACKET):
                subquery = self._parse_subquery()
                node.subqueries.append(subquery)
                continue

            # Check for BY clause
            if self._match(TokenType.BY):
                self._advance()
                while not self._match(TokenType.PIPE, TokenType.EOF):
                    if self._match(TokenType.IDENTIFIER):
                        node.by_fields.append(self._advance().value)
                        if self._match(TokenType.COMMA):
                            self._advance()
                    else:
                        break
                continue

            # Positional argument
            if self._match(TokenType.IDENTIFIER, TokenType.STRING, TokenType.NUMBER):
                value = self._parse_value()
                node.arguments.append(
                    PositionalArgumentNode(value=value, position=self._current_token().position)
                )
            else:
                break

    def _parse_expression(self) -> ASTNode:
        """Parse an expression (for function arguments, etc.)."""
        return self._parse_additive()

    def _parse_additive(self) -> ASTNode:
        """Parse additive expression: term ((+|-) term)*"""
        left = self._parse_multiplicative()

        while self._match(TokenType.PLUS, TokenType.MINUS):
            op = self._advance().value
            right = self._parse_multiplicative()
            left = BinaryOpNode(left=left, operator=op, right=right, position=self._current_token().position)

        return left

    def _parse_multiplicative(self) -> ASTNode:
        """Parse multiplicative expression: primary ((*|/) primary)*"""
        left = self._parse_primary()

        while self._match(TokenType.STAR, TokenType.SLASH):
            op = self._advance().value
            right = self._parse_primary()
            left = BinaryOpNode(left=left, operator=op, right=right, position=self._current_token().position)

        return left

    def _parse_primary(self) -> ASTNode:
        """Parse primary expression: literal, identifier, function call, or parenthesized expression."""
        position = self._current_token().position

        # Parenthesized expression
        if self._match(TokenType.LPAREN):
            self._advance()
            expr = self._parse_expression()
            self._expect(TokenType.RPAREN)
            return expr

        # String literal
        if self._match(TokenType.STRING):
            token = self._advance()
            return LiteralNode(value=token.value, literal_type="string", position=position)

        # Number literal
        if self._match(TokenType.NUMBER):
            token = self._advance()
            # Determine if int or float
            value = float(token.value) if "." in token.value else int(token.value)
            return LiteralNode(value=value, literal_type="number", position=position)

        # Identifier or function call
        if self._match(TokenType.IDENTIFIER):
            name = self._advance().value

            # Check for function call
            if self._match(TokenType.LPAREN):
                self._advance()  # consume (
                args: list[ASTNode] = []

                while not self._match(TokenType.RPAREN):
                    arg = self._parse_expression()
                    args.append(arg)

                    if self._match(TokenType.COMMA):
                        self._advance()
                    elif not self._match(TokenType.RPAREN):
                        break

                self._expect(TokenType.RPAREN)
                return FunctionCallNode(name=name, arguments=args, position=position)

            return IdentifierNode(name=name, position=position)

        raise ParserError(f"Unexpected token: {self._current_token()}", self._current_token())

    def _parse_value(self) -> ASTNode:
        """Parse a single value (string, number, identifier, or function call)."""
        return self._parse_primary()

    def _parse_value_as_string(self) -> str:
        """Parse a value and return its string representation."""
        if self._match(TokenType.STRING):
            return self._advance().value
        elif self._match(TokenType.NUMBER):
            return self._advance().value
        elif self._match(TokenType.IDENTIFIER):
            return self._advance().value
        else:
            raise ParserError("Expected value", self._current_token())

