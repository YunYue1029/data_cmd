"""
Pipe Commands module.

This module contains all available pipe command implementations.
Commands are automatically registered via the @PipeMap.register decorator.
"""

from RDP.pipe.commands.base import PipeCommand

# Data retrieval commands
from RDP.pipe.commands.cache import CacheCommand, NewCacheCommand
from RDP.pipe.commands.search import SearchCommand
from RDP.pipe.commands.lookup import LookupCommand

# Row selection commands
from RDP.pipe.commands.filter import FilterCommand
from RDP.pipe.commands.head import HeadCommand
from RDP.pipe.commands.tail import TailCommand
from RDP.pipe.commands.sample import SampleCommand
from RDP.pipe.commands.dedup import DedupCommand
from RDP.pipe.commands.dropnull import DropNullCommand

# Column operations
from RDP.pipe.commands.select import SelectCommand
from RDP.pipe.commands.rename import RenameCommand
from RDP.pipe.commands.eval import EvalCommand

# Aggregation commands
from RDP.pipe.commands.stats import StatsCommand
from RDP.pipe.commands.top import TopCommand
from RDP.pipe.commands.rare import RareCommand

# Transformation commands
from RDP.pipe.commands.sort import SortCommand
from RDP.pipe.commands.reverse import ReverseCommand
from RDP.pipe.commands.transpose import TransposeCommand
from RDP.pipe.commands.fillnull import FillNullCommand
from RDP.pipe.commands.replace import ReplaceCommand
from RDP.pipe.commands.mvexpand import MvExpandCommand
from RDP.pipe.commands.rex import RexCommand

# Join commands
from RDP.pipe.commands.join import JoinCommand

# Multi-source commands
from RDP.pipe.commands.append import AppendCommand

# Time analysis commands
from RDP.pipe.commands.bucket import BucketCommand
from RDP.pipe.commands.transaction import TransactionCommand

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
    # Time analysis
    "BucketCommand",
    "TransactionCommand",
    # Multi-source
    "AppendCommand",
]
