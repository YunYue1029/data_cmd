"""
Lexer module for tokenizing command strings.

This module provides tokenization for the command pipeline syntax,
handling pipe operators, brackets, quotes, and other special characters.
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import Iterator


class TokenType(Enum):
    """Token types for the command lexer."""

    # Literals
    IDENTIFIER = auto()  # command names, field names
    STRING = auto()  # quoted strings
    NUMBER = auto()  # numeric literals

    # Operators
    PIPE = auto()  # |
    EQUALS = auto()  # =
    COMMA = auto()  # ,
    DOT = auto()  # .

    # Comparison operators
    GT = auto()  # >
    LT = auto()  # <
    GTE = auto()  # >=
    LTE = auto()  # <=
    EQ = auto()  # ==
    NEQ = auto()  # !=

    # Arithmetic operators
    PLUS = auto()  # +
    MINUS = auto()  # -
    STAR = auto()  # *
    SLASH = auto()  # /

    # Brackets
    LPAREN = auto()  # (
    RPAREN = auto()  # )
    LBRACKET = auto()  # [
    RBRACKET = auto()  # ]

    # Keywords
    BY = auto()  # by (for stats, top, etc.)
    AS = auto()  # as (for aliasing)
    WITH = auto()  # with (for replace)
    IN = auto()  # in (for replace)
    AND = auto()  # and (logical)
    OR = auto()  # or (logical)

    # Special
    EOF = auto()
    NEWLINE = auto()


# Keywords mapping
KEYWORDS = {
    "by": TokenType.BY,
    "as": TokenType.AS,
    "with": TokenType.WITH,
    "in": TokenType.IN,
    "and": TokenType.AND,
    "or": TokenType.OR,
}


@dataclass
class Token:
    """Represents a single token from the lexer."""

    type: TokenType
    value: str
    position: int  # starting position in the source string
    line: int = 1
    column: int = 0

    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r}, pos={self.position})"


class LexerError(Exception):
    """Exception raised for lexer errors."""

    def __init__(self, message: str, position: int, line: int = 1, column: int = 0):
        self.position = position
        self.line = line
        self.column = column
        super().__init__(f"{message} at line {line}, column {column}")


class CommandLexer:
    """
    Tokenizer for command pipeline syntax.

    Handles:
    - Pipe operators (|) with proper handling inside brackets/quotes
    - Quoted strings (single and double quotes)
    - Bracketed expressions [...]
    - Identifiers and keywords
    - Numbers (integers and floats)
    - Operators and punctuation
    """

    def __init__(self, source: str):
        self.source = source
        self.pos = 0
        self.line = 1
        self.column = 0
        self.length = len(source)

    def _current_char(self) -> str | None:
        """Return current character or None if at end."""
        if self.pos >= self.length:
            return None
        return self.source[self.pos]

    def _peek_char(self, offset: int = 1) -> str | None:
        """Peek at character at given offset from current position."""
        peek_pos = self.pos + offset
        if peek_pos >= self.length:
            return None
        return self.source[peek_pos]

    def _advance(self) -> str | None:
        """Advance position and return the character."""
        char = self._current_char()
        if char is not None:
            self.pos += 1
            if char == "\n":
                self.line += 1
                self.column = 0
            else:
                self.column += 1
        return char

    def _skip_whitespace(self) -> None:
        """Skip whitespace characters (except newlines if needed)."""
        while self._current_char() is not None and self._current_char() in " \t\r\n":
            self._advance()

    def _read_string(self, quote_char: str) -> Token:
        """Read a quoted string."""
        start_pos = self.pos
        start_column = self.column
        self._advance()  # skip opening quote

        value_chars: list[str] = []
        while True:
            char = self._current_char()
            if char is None:
                raise LexerError(
                    f"Unterminated string starting with {quote_char}",
                    start_pos,
                    self.line,
                    start_column,
                )
            if char == quote_char:
                self._advance()  # skip closing quote
                break
            if char == "\\":
                self._advance()  # skip backslash
                escaped = self._current_char()
                if escaped is not None:
                    # Handle escape sequences
                    # For known escape sequences, translate them
                    # For unknown sequences (like \d in regex), preserve the backslash
                    escape_map = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "'": "'"}
                    if escaped in escape_map:
                        value_chars.append(escape_map[escaped])
                    else:
                        # Preserve backslash for regex patterns like \d, \w, \s, etc.
                        value_chars.append("\\")
                        value_chars.append(escaped)
                    self._advance()
            else:
                value_chars.append(char)
                self._advance()

        return Token(
            type=TokenType.STRING,
            value="".join(value_chars),
            position=start_pos,
            line=self.line,
            column=start_column,
        )

    def _read_number(self) -> Token:
        """Read a numeric literal (integer or float)."""
        start_pos = self.pos
        start_column = self.column
        chars: list[str] = []

        # Handle negative numbers
        if self._current_char() == "-":
            chars.append("-")
            self._advance()

        # Read integer part
        while self._current_char() is not None and self._current_char().isdigit():
            chars.append(self._advance())  # type: ignore

        # Check for decimal point
        if self._current_char() == "." and (
            self._peek_char() is not None and self._peek_char().isdigit()  # type: ignore
        ):
            chars.append(self._advance())  # type: ignore  # add decimal point
            while self._current_char() is not None and self._current_char().isdigit():
                chars.append(self._advance())  # type: ignore

        return Token(
            type=TokenType.NUMBER,
            value="".join(chars),
            position=start_pos,
            line=self.line,
            column=start_column,
        )

    def _read_identifier(self) -> Token:
        """Read an identifier or keyword."""
        start_pos = self.pos
        start_column = self.column
        chars: list[str] = []

        # First character: letter or underscore
        while self._current_char() is not None and (
            self._current_char().isalnum() or self._current_char() in "_"
        ):
            chars.append(self._advance())  # type: ignore

        value = "".join(chars)
        lower_value = value.lower()

        # Check if it's a keyword
        if lower_value in KEYWORDS:
            return Token(
                type=KEYWORDS[lower_value],
                value=value,
                position=start_pos,
                line=self.line,
                column=start_column,
            )

        return Token(
            type=TokenType.IDENTIFIER,
            value=value,
            position=start_pos,
            line=self.line,
            column=start_column,
        )

    def tokenize(self) -> list[Token]:
        """Tokenize the entire source string."""
        tokens: list[Token] = []

        while self.pos < self.length:
            self._skip_whitespace()

            if self.pos >= self.length:
                break

            char = self._current_char()
            start_pos = self.pos
            start_column = self.column

            # String literals
            if char in '"\'':
                tokens.append(self._read_string(char))
                continue

            # Numbers (including negative numbers)
            if char.isdigit() or (
                char == "-"
                and self._peek_char() is not None
                and self._peek_char().isdigit()  # type: ignore
            ):
                tokens.append(self._read_number())
                continue

            # Identifiers and keywords
            if char.isalpha() or char == "_":
                tokens.append(self._read_identifier())
                continue

            # Single-character tokens
            single_char_tokens = {
                "|": TokenType.PIPE,
                ",": TokenType.COMMA,
                ".": TokenType.DOT,
                "(": TokenType.LPAREN,
                ")": TokenType.RPAREN,
                "[": TokenType.LBRACKET,
                "]": TokenType.RBRACKET,
                "+": TokenType.PLUS,
                "*": TokenType.STAR,
                "/": TokenType.SLASH,
            }

            if char in single_char_tokens:
                self._advance()
                tokens.append(
                    Token(
                        type=single_char_tokens[char],
                        value=char,
                        position=start_pos,
                        line=self.line,
                        column=start_column,
                    )
                )
                continue

            # Multi-character operators
            if char == "=":
                self._advance()
                if self._current_char() == "=":
                    self._advance()
                    tokens.append(
                        Token(TokenType.EQ, "==", start_pos, self.line, start_column)
                    )
                else:
                    tokens.append(
                        Token(TokenType.EQUALS, "=", start_pos, self.line, start_column)
                    )
                continue

            if char == "!":
                self._advance()
                if self._current_char() == "=":
                    self._advance()
                    tokens.append(
                        Token(TokenType.NEQ, "!=", start_pos, self.line, start_column)
                    )
                else:
                    raise LexerError(
                        f"Unexpected character: {char}", start_pos, self.line, start_column
                    )
                continue

            if char == ">":
                self._advance()
                if self._current_char() == "=":
                    self._advance()
                    tokens.append(
                        Token(TokenType.GTE, ">=", start_pos, self.line, start_column)
                    )
                else:
                    tokens.append(
                        Token(TokenType.GT, ">", start_pos, self.line, start_column)
                    )
                continue

            if char == "<":
                self._advance()
                if self._current_char() == "=":
                    self._advance()
                    tokens.append(
                        Token(TokenType.LTE, "<=", start_pos, self.line, start_column)
                    )
                else:
                    tokens.append(
                        Token(TokenType.LT, "<", start_pos, self.line, start_column)
                    )
                continue

            if char == "-":
                self._advance()
                tokens.append(
                    Token(TokenType.MINUS, "-", start_pos, self.line, start_column)
                )
                continue

            raise LexerError(
                f"Unexpected character: {char!r}", start_pos, self.line, start_column
            )

        # Add EOF token
        tokens.append(
            Token(TokenType.EOF, "", self.pos, self.line, self.column)
        )

        return tokens

    def tokenize_iter(self) -> Iterator[Token]:
        """Tokenize as an iterator (memory efficient for large inputs)."""
        for token in self.tokenize():
            yield token


def split_by_pipe(source: str) -> list[str]:
    """
    Split command string by pipe operators, respecting brackets and quotes.

    This is a utility function for quick pipeline splitting without full tokenization.

    Args:
        source: The command string to split

    Returns:
        List of command segments (without the pipe operators)

    Example:
        >>> split_by_pipe("cmd1 | cmd2 [sub | cmd] | cmd3")
        ['cmd1 ', ' cmd2 [sub | cmd] ', ' cmd3']
    """
    segments: list[str] = []
    current: list[str] = []
    bracket_depth = 0
    in_string = False
    string_char = None
    i = 0

    while i < len(source):
        char = source[i]

        # Handle string state
        if in_string:
            current.append(char)
            if char == "\\" and i + 1 < len(source):
                # Skip escaped character
                i += 1
                current.append(source[i])
            elif char == string_char:
                in_string = False
                string_char = None
        elif char in '"\'':
            in_string = True
            string_char = char
            current.append(char)
        elif char == "[":
            bracket_depth += 1
            current.append(char)
        elif char == "]":
            bracket_depth -= 1
            current.append(char)
        elif char == "|" and bracket_depth == 0:
            # Found a pipe at top level
            segments.append("".join(current))
            current = []
        else:
            current.append(char)

        i += 1

    # Add the last segment
    if current:
        segments.append("".join(current))

    return segments

