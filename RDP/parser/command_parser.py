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
        
        Special handling: In LIKE expressions, wildcards (* and %) should be
        merged with adjacent identifiers if they're not in quotes.
        """
        # Collect all tokens with their types for processing
        token_list = []
        start_pos = self._current_token().position
        
        while not self._match(TokenType.PIPE, TokenType.EOF):
            token = self._advance()
            token_list.append(token)
        
        if not token_list:
            return
        
        # Check if this is a LIKE expression and merge wildcards
        # Look for LIKE keyword in the tokens
        like_index = None
        for i, token in enumerate(token_list):
            if token.type == TokenType.IDENTIFIER and token.value.upper() == "LIKE":
                like_index = i
                break
        
        # If LIKE found, merge wildcards (* and %) with adjacent identifiers
        if like_index is not None and like_index + 1 < len(token_list):
            # Process tokens after LIKE
            result_tokens = token_list[:like_index + 1]  # Keep everything up to and including LIKE
            i = like_index + 1
            
            while i < len(token_list):
                token = token_list[i]
                
                # If token is a string literal, keep it as is
                if token.type == TokenType.STRING:
                    result_tokens.append(token)
                    i += 1
                    continue
                
                # If token is a wildcard (* or %), merge with adjacent identifiers
                if token.type == TokenType.STAR or (token.type == TokenType.IDENTIFIER and token.value == "%"):
                    # Check if previous token is an identifier (but not LIKE keyword)
                    if (result_tokens and 
                        result_tokens[-1].type == TokenType.IDENTIFIER and
                        result_tokens[-1].value.upper() != "LIKE"):
                        # Merge wildcard with previous identifier
                        prev_token = result_tokens.pop()
                        merged_value = prev_token.value + token.value
                        # Create a new token with merged value
                        merged_token = Token(
                            type=TokenType.IDENTIFIER,
                            value=merged_value,
                            position=prev_token.position,
                            line=prev_token.line,
                            column=prev_token.column,
                        )
                        result_tokens.append(merged_token)
                    else:
                        # No previous identifier (or previous is LIKE), keep wildcard as is
                        result_tokens.append(token)
                    
                    # Check if next token is an identifier and merge
                    if i + 1 < len(token_list) and token_list[i + 1].type == TokenType.IDENTIFIER:
                        next_token = token_list[i + 1]
                        last_token = result_tokens[-1]
                        merged_value = last_token.value + next_token.value
                        result_tokens[-1] = Token(
                            type=TokenType.IDENTIFIER,
                            value=merged_value,
                            position=last_token.position,
                            line=last_token.line,
                            column=last_token.column,
                        )
                        i += 2  # Skip both wildcard and next token
                        continue
                    
                    i += 1
                    continue
                
                # For other tokens, check if they should be merged with previous wildcard
                if (token.type == TokenType.IDENTIFIER and 
                    result_tokens and 
                    (result_tokens[-1].type == TokenType.STAR or 
                     (result_tokens[-1].type == TokenType.IDENTIFIER and result_tokens[-1].value.endswith("%")))):
                    # Merge identifier with previous wildcard
                    prev_token = result_tokens.pop()
                    merged_value = prev_token.value + token.value
                    result_tokens.append(Token(
                        type=TokenType.IDENTIFIER,
                        value=merged_value,
                        position=prev_token.position,
                        line=prev_token.line,
                        column=prev_token.column,
                    ))
                    i += 1
                    continue
                
                # Keep token as is
                result_tokens.append(token)
                i += 1
            
            token_list = result_tokens
        
        # Convert tokens to string values
        tokens = [token.value for token in token_list]
        
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

            # Handle + or - prefix (for fields command, etc.)
            prefix = ""
            if self._match(TokenType.PLUS, TokenType.MINUS):
                prefix = self._advance().value

            # Positional argument - collect tokens to handle wildcards
            if self._match(TokenType.IDENTIFIER, TokenType.STRING, TokenType.NUMBER):
                # Collect tokens for the argument value, merging wildcards
                arg_tokens = []
                start_pos = self._current_token().position
                
                # First token
                token = self._advance()
                arg_tokens.append(token)
                
                # Collect subsequent tokens that are part of the same argument
                # (merge wildcards with identifiers)
                while not self._match(TokenType.PIPE, TokenType.EOF):
                    # Stop at comma (separates arguments)
                    if self._match(TokenType.COMMA):
                        break
                    
                    # If next token is a wildcard (*), merge with previous identifier
                    if self._match(TokenType.STAR):
                        if arg_tokens and arg_tokens[-1].type == TokenType.IDENTIFIER:
                            # Merge * with previous identifier
                            prev_token = arg_tokens.pop()
                            merged_value = prev_token.value + "*"
                            from RDP.lexer import Token
                            arg_tokens.append(
                                Token(
                                    TokenType.IDENTIFIER,
                                    merged_value,
                                    prev_token.position,
                                    prev_token.line,
                                    prev_token.column,
                                )
                            )
                            self._advance()  # consume *
                        else:
                            # Standalone *, add as is
                            arg_tokens.append(self._advance())
                    # If next token is an identifier and previous is a wildcard, merge
                    elif self._match(TokenType.IDENTIFIER):
                        if arg_tokens and arg_tokens[-1].type == TokenType.STAR:
                            # Merge identifier with previous *
                            prev_token = arg_tokens.pop()
                            next_token = self._advance()
                            merged_value = prev_token.value + next_token.value
                            from RDP.lexer import Token
                            arg_tokens.append(
                                Token(
                                    TokenType.IDENTIFIER,
                                    merged_value,
                                    prev_token.position,
                                    prev_token.line,
                                    prev_token.column,
                                )
                            )
                        else:
                            # Regular identifier, but check if it's part of current argument
                            # (e.g., for patterns like col_*pattern, we want to merge)
                            peek = self._peek_token()
                            if peek.type == TokenType.STAR:
                                # Next is *, so this is part of the pattern, continue collecting
                                arg_tokens.append(self._advance())
                            else:
                                # Next is not *, this is a new argument
                                break
                    else:
                        # Not part of current argument
                        break
                
                # Skip comma if present (for next iteration)
                if self._match(TokenType.COMMA):
                    self._advance()
                
                # Build the argument value from collected tokens
                if arg_tokens:
                    # Combine token values, handling prefix
                    if len(arg_tokens) == 1:
                        if arg_tokens[0].type == TokenType.STRING:
                            # String literal, use as is
                            value = LiteralNode(
                                value=prefix + arg_tokens[0].value,
                                literal_type="string"
                            )
                        elif arg_tokens[0].type == TokenType.NUMBER:
                            # Number, prefix doesn't make sense, but handle it
                            value = LiteralNode(
                                value=arg_tokens[0].value,
                                literal_type="number"
                            )
                        else:
                            # Identifier
                            value = IdentifierNode(name=prefix + arg_tokens[0].value)
                    else:
                        # Multiple tokens merged (e.g., col_* -> col_*)
                        merged_value = prefix + "".join(t.value for t in arg_tokens)
                        value = IdentifierNode(name=merged_value)
                    
                    node.arguments.append(
                        PositionalArgumentNode(value=value, position=start_pos)
                    )
            elif prefix:
                # Had a prefix but no valid argument following
                raise ParserError(f"Expected argument after '{prefix}'", self._current_token())
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

