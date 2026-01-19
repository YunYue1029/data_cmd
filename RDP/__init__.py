"""
Data CMD - A Splunk-like data processing command system.

This package provides a command pipeline syntax for processing
pandas DataFrames, similar to Splunk's search language.

Usage:
    from data_cmd import CommandExecutor, register_cache

    # Register your DataFrame
    register_cache("my_data", df)

    # Execute a command
    result = CommandExecutor("cache=my_data | stats count by category").execute()
"""

# Lazy imports to avoid circular import issues
def __getattr__(name: str):
    if name == "CommandExecutor":
        from RDP.executors import CommandExecutor
        return CommandExecutor
    if name == "register_cache":
        from RDP.executors import register_cache
        return register_cache
    if name == "clear_cache":
        from RDP.executors import clear_cache
        return clear_cache
    if name == "list_cache":
        from RDP.executors import list_cache
        return list_cache
    if name == "DataFrameCache":
        from RDP.pipe.commands.cache import DataFrameCache
        return DataFrameCache
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CommandExecutor",
    "DataFrameCache",
    "register_cache",
    "clear_cache",
    "list_cache",
]

__version__ = "0.1.0"

