"""
Reverse command - Reverse the order of rows.
"""

from typing import Any

import pandas as pd

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class ReverseCommand(PipeCommand):
    """
    Reverse command for reversing row order.

    Usage:
        reverse
    """

    keywords = ["reverse"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reverse the order of rows."""
        if df.empty:
            return df
        return df.iloc[::-1].reset_index(drop=True)

