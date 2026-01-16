"""
Pipe Commands module.

This module contains all available pipe command implementations.
Commands are automatically registered via the @PipeMap.register decorator.
"""

from pipe.commands.base import PipeCommand

# Data retrieval commands
from pipe.commands.cache import CacheCommand, NewCacheCommand
from pipe.commands.search import SearchCommand
from pipe.commands.lookup import LookupCommand

# Row selection commands
from pipe.commands.filter import FilterCommand
from pipe.commands.head import HeadCommand
from pipe.commands.tail import TailCommand
from pipe.commands.sample import SampleCommand
from pipe.commands.dedup import DedupCommand
from pipe.commands.dropnull import DropNullCommand

# Column operations
from pipe.commands.select import SelectCommand
from pipe.commands.rename import RenameCommand
from pipe.commands.eval import EvalCommand

# Aggregation commands
from pipe.commands.stats import StatsCommand
from pipe.commands.top import TopCommand
from pipe.commands.rare import RareCommand

# Transformation commands
from pipe.commands.sort import SortCommand
from pipe.commands.reverse import ReverseCommand
from pipe.commands.transpose import TransposeCommand
from pipe.commands.fillnull import FillNullCommand
from pipe.commands.replace import ReplaceCommand
from pipe.commands.mvexpand import MvExpandCommand
from pipe.commands.rex import RexCommand

# Join commands
from pipe.commands.join import JoinCommand

__all__ = [
    # Base
    "PipeCommand",
    # Data retrieval
    "CacheCommand",
    "NewCacheCommand",
    "SearchCommand",
    "LookupCommand",
    # Row selection
    "FilterCommand",
    "HeadCommand",
    "TailCommand",
    "SampleCommand",
    "DedupCommand",
    "DropNullCommand",
    # Column operations
    "SelectCommand",
    "RenameCommand",
    "EvalCommand",
    # Aggregation
    "StatsCommand",
    "TopCommand",
    "RareCommand",
    # Transformation
    "SortCommand",
    "ReverseCommand",
    "TransposeCommand",
    "FillNullCommand",
    "ReplaceCommand",
    "MvExpandCommand",
    "RexCommand",
    # Join
    "JoinCommand",
]
