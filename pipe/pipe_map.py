"""
Pipe Map - Registry for command classes.

This module provides a decorator-based registration system
for mapping command keywords to their implementation classes.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipe.commands.base import PipeCommand


class PipeMap:
    """
    Registry for mapping command keywords to command classes.

    Usage:
        @PipeMap.register
        class MyCommand(PipeCommand):
            keywords = ["mycommand", "mycmd"]
            ...
    """

    _registry: dict[str, type["PipeCommand"]] = {}

    @classmethod
    def register(cls, command_class: type["PipeCommand"]) -> type["PipeCommand"]:
        """
        Register a command class with its keywords.

        Args:
            command_class: The PipeCommand subclass to register

        Returns:
            The same command class (for use as a decorator)
        """
        keywords = getattr(command_class, "keywords", [])
        if not keywords:
            # Use class name as default keyword
            name = command_class.__name__.lower()
            if name.endswith("command"):
                name = name[:-7]  # Remove 'command' suffix
            keywords = [name]

        for keyword in keywords:
            cls._registry[keyword.lower()] = command_class

        return command_class

    @classmethod
    def get(cls, keyword: str) -> type["PipeCommand"] | None:
        """
        Get the command class for a keyword.

        Args:
            keyword: The command keyword to look up

        Returns:
            The command class or None if not found
        """
        return cls._registry.get(keyword.lower())

    @classmethod
    def list(cls) -> list[str]:
        """
        List all registered command keywords.

        Returns:
            List of registered keywords
        """
        return sorted(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered commands (mainly for testing)."""
        cls._registry.clear()

