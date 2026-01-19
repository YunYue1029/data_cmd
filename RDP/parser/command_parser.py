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

        # Check for multi-source pattern: (index="a" OR index="b")
        if self._match(TokenType.LPAREN):
            return self._parse_multi_source(position)

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

    def _parse_multi_source(self, position: int) -> SourceNode:
        """
        Parse multi-source specification: (index="a" OR index="b")
        
        Supports:
            (index="a" OR index="b")
            (cache=data1 OR cache=data2)
        """
        self._advance()  # consume (
        
        sources: list[SourceNode] = []
        
        while not self._match(TokenType.RPAREN, TokenType.EOF):
            # Parse single source: index="name" or cache=name
            if self._match(TokenType.IDENTIFIER):
                source_type = self._advance().value.lower()
                
                if self._match(TokenType.EQUALS):
                    self._advance()  # consume =
                    source_name = self._parse_value_as_string()
                    sources.append(SourceNode(
                        source_type=source_type,
                        source_name=source_name,
                        position=self._current_token().position,
                    ))
                else:
                    raise ParserError(
                        f"Expected = after {source_type}", 
                        self._current_token()
                    )
            
            # Check for OR to continue
            if self._match(TokenType.OR):
                self._advance()  # consume OR
            elif not self._match(TokenType.RPAREN):
                break
        
        self._expect(TokenType.RPAREN)
        
        if not sources:
            raise ParserError("Empty multi-source specification", self._current_token())
        
        if len(sources) == 1:
            # Single source, return it directly
            return sources[0]
        
        # Multiple sources
        return SourceNode(
            source_type="multi",
            source_name="",
            multi_sources=sources,
            position=position,
        )

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
        elif cmd_name.lower() in ("eval", "calculate", "compute"):
            self._parse_eval_arguments(node)
        elif cmd_name.lower() == "sort":
            self._parse_sort_arguments(node)
        elif cmd_name.lower() == "join":
            self._parse_join_arguments(node)
        elif cmd_name.lower() == "head":
            self._parse_head_arguments(node)
        elif cmd_name.lower() in ("filter", "where"):
            self._parse_where_arguments(node)
        elif cmd_name.lower() in ("bucket", "bin"):
            self._parse_bucket_arguments(node)
        elif cmd_name.lower() == "transaction":
            self._parse_transaction_arguments(node)
        elif cmd_name.lower() == "search":
            self._parse_search_arguments(node)
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

    def _parse_where_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse where/filter command arguments as raw expression string.
        
        Format: where <boolean_expression>
        
        Examples:
            where status_code >= 400 AND status_code < 500
            where uri LIKE "%api%"
            where status_code IN (200, 201, 404)
            where isnull(value)
        
        This method collects all tokens until PIPE or EOF and stores them
        as a raw string for the FilterCommand to evaluate.
        """
        # Collect all tokens as a raw expression string
        tokens = []
        start_pos = self._current_token().position
        
        while not self._match(TokenType.PIPE, TokenType.EOF):
            token = self._advance()
            tokens.append(token.value)
        
        if tokens:
            # Join tokens to form the expression string
            expr_str = " ".join(tokens)
            node.arguments.append(
                PositionalArgumentNode(
                    value=LiteralNode(value=expr_str, literal_type="string"),
                    position=start_pos,
                )
            )

    def _parse_bucket_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse bucket command arguments.
        Format: bucket field span=5m
        
        Handles time span format like 5m, 1h, 30s, 1d where number
        and unit are parsed separately by the lexer.
        """
        # First, parse the field name (positional)
        if self._match(TokenType.IDENTIFIER):
            field_name = self._advance().value
            node.arguments.append(
                PositionalArgumentNode(
                    value=IdentifierNode(name=field_name),
                    position=self._current_token().position,
                )
            )

        # Parse keyword arguments (span=5m)
        while not self._match(TokenType.PIPE, TokenType.EOF):
            if self._match(TokenType.IDENTIFIER):
                if self._peek_token().type == TokenType.EQUALS:
                    key = self._advance().value
                    self._advance()  # consume =

                    # For span argument, collect number + unit
                    if key == "span" and self._match(TokenType.NUMBER):
                        num_token = self._advance()
                        span_value = str(num_token.value)

                        # Check if next token is a unit identifier (s, m, h, d, w)
                        if self._match(TokenType.IDENTIFIER):
                            unit_token = self._advance()
                            unit = unit_token.value.lower()
                            if unit in ("s", "m", "h", "d", "w"):
                                span_value += unit

                        node.arguments.append(
                            KeywordArgumentNode(
                                key=key,
                                value=LiteralNode(value=span_value, literal_type="string"),
                                position=self._current_token().position,
                            )
                        )
                    else:
                        # Regular keyword argument
                        value = self._parse_value()
                        node.arguments.append(
                            KeywordArgumentNode(
                                key=key,
                                value=value,
                                position=self._current_token().position,
                            )
                        )
                else:
                    # Just an identifier without =, could be additional field
                    break
            else:
                break

    def _parse_transaction_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse transaction command arguments.
        Format: transaction group_field maxspan=5m
        
        Handles time span format like 5m, 1h, 30s where number
        and unit are parsed separately by the lexer.
        """
        # First, parse the group field (positional)
        if self._match(TokenType.IDENTIFIER):
            field_name = self._advance().value
            node.arguments.append(
                PositionalArgumentNode(
                    value=IdentifierNode(name=field_name),
                    position=self._current_token().position,
                )
            )

        # Parse keyword arguments (maxspan=5m)
        while not self._match(TokenType.PIPE, TokenType.EOF):
            if self._match(TokenType.IDENTIFIER):
                if self._peek_token().type == TokenType.EQUALS:
                    key = self._advance().value
                    self._advance()  # consume =

                    # For maxspan argument, collect number + unit
                    if key == "maxspan" and self._match(TokenType.NUMBER):
                        num_token = self._advance()
                        span_value = str(num_token.value)

                        # Check if next token is a unit identifier (s, m, h, d, w)
                        if self._match(TokenType.IDENTIFIER):
                            unit_token = self._advance()
                            unit = unit_token.value.lower()
                            if unit in ("s", "m", "h", "d", "w"):
                                span_value += unit

                        node.arguments.append(
                            KeywordArgumentNode(
                                key=key,
                                value=LiteralNode(value=span_value, literal_type="string"),
                                position=self._current_token().position,
                            )
                        )
                    else:
                        # Regular keyword argument
                        value = self._parse_value()
                        node.arguments.append(
                            KeywordArgumentNode(
                                key=key,
                                value=value,
                                position=self._current_token().position,
                            )
                        )
                else:
                    # Just an identifier without =
                    break
            else:
                break

    def _parse_search_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse search command arguments.
        Format: search index="name" latest=-5m earliest="2024-01-01"
        
        Handles time span format like -5m, -1h where number and unit
        may be parsed separately by the lexer.
        """
        while not self._match(TokenType.PIPE, TokenType.EOF):
            if self._match(TokenType.IDENTIFIER):
                if self._peek_token().type == TokenType.EQUALS:
                    key = self._advance().value
                    self._advance()  # consume =

                    # For latest/earliest arguments, handle relative time format
                    if key in ("latest", "earliest"):
                        # Check for NUMBER (which may be negative like -5)
                        if self._match(TokenType.NUMBER):
                            num_token = self._advance()
                            time_value = str(num_token.value)

                            # Check if next token is a unit identifier (s, m, h, d, w)
                            if self._match(TokenType.IDENTIFIER):
                                unit_token = self._advance()
                                unit = unit_token.value.lower()
                                if unit in ("s", "m", "h", "d", "w"):
                                    time_value += unit

                            node.arguments.append(
                                KeywordArgumentNode(
                                    key=key,
                                    value=LiteralNode(value=time_value, literal_type="string"),
                                    position=self._current_token().position,
                                )
                            )
                            continue
                        elif self._match(TokenType.STRING):
                            # Absolute time string
                            value_token = self._advance()
                            node.arguments.append(
                                KeywordArgumentNode(
                                    key=key,
                                    value=LiteralNode(value=value_token.value, literal_type="string"),
                                    position=self._current_token().position,
                                )
                            )
                            continue

                    # Regular keyword argument
                    value = self._parse_value()
                    node.arguments.append(
                        KeywordArgumentNode(
                            key=key,
                            value=value,
                            position=self._current_token().position,
                        )
                    )
                else:
                    # Just an identifier without =
                    break
            else:
                break

    def _parse_eval_arguments(self, node: PipeCommandNode) -> None:
        """
        Parse eval command arguments.
        Format: eval field=expression, field2=expression2, ...
        
        Expressions can contain arithmetic operations, function calls, etc.
        """
        while not self._match(TokenType.PIPE, TokenType.EOF):
            # Expect: field_name = expression
            if self._match(TokenType.IDENTIFIER):
                field_name = self._advance().value
                
                if self._match(TokenType.EQUALS):
                    self._advance()  # consume =
                    # Parse the full expression (not just a simple value)
                    expr = self._parse_expression()
                    node.arguments.append(
                        KeywordArgumentNode(
                            key=field_name,
                            value=expr,
                            position=self._current_token().position,
                        )
                    )
                    
                    # Check for comma (multiple assignments)
                    if self._match(TokenType.COMMA):
                        self._advance()
                        continue
                else:
                    # Not an assignment, might be end of eval args
                    break
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
        return self._parse_or_expression()

    def _parse_or_expression(self) -> ASTNode:
        """Parse OR expression: and_expr (OR and_expr)*"""
        left = self._parse_and_expression()

        while self._match(TokenType.OR):
            self._advance()  # consume OR
            right = self._parse_and_expression()
            left = BinaryOpNode(left=left, operator="OR", right=right,
                               position=self._current_token().position)

        return left

    def _parse_and_expression(self) -> ASTNode:
        """Parse AND expression: comparison (AND comparison)*"""
        left = self._parse_comparison()

        while self._match(TokenType.AND):
            self._advance()  # consume AND
            right = self._parse_comparison()
            left = BinaryOpNode(left=left, operator="AND", right=right,
                               position=self._current_token().position)

        return left

    def _parse_comparison(self) -> ASTNode:
        """Parse comparison expression: additive ((>|<|>=|<=|==|!=|=) additive)*"""
        left = self._parse_additive()

        # Include EQUALS for single = comparisons (common in Splunk syntax like 1=1)
        while self._match(TokenType.GT, TokenType.LT, TokenType.GTE, 
                         TokenType.LTE, TokenType.EQ, TokenType.NEQ, TokenType.EQUALS):
            op_token = self._advance()
            # Normalize single = to == for comparison
            op = "==" if op_token.type == TokenType.EQUALS else op_token.value
            right = self._parse_additive()
            left = BinaryOpNode(left=left, operator=op, right=right, 
                               position=self._current_token().position)

        return left

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

