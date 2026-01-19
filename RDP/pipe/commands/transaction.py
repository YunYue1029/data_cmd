"""
Transaction command - Group events into transactions/sessions.

Groups events by a field (e.g., user_id, session_id) and splits into
separate transactions when the time gap between events exceeds maxspan.
"""

import re
from typing import Any

import pandas as pd
import numpy as np

from RDP.pipe.commands.base import PipeCommand
from RDP.pipe.pipe_map import PipeMap


@PipeMap.register
class TransactionCommand(PipeCommand):
    """
    Transaction command for session/event correlation.

    Groups events into transactions based on a grouping field and
    splits transactions when the time gap exceeds maxspan.

    Usage:
        transaction user_id maxspan=5m
        transaction session_id maxspan=30m
        transaction user_id maxspan=1h | stats count as session_count by user_id
    """

    keywords = ["transaction"]

    def __init__(self, args: list[str] | None = None, **kwargs: Any):
        super().__init__(args, **kwargs)
        self.group_field: str = kwargs.get("group_field", "")
        self.maxspan: str = kwargs.get("maxspan", "5m")
        self.time_field: str = kwargs.get("time_field", "_time")

        # Parse from AST node if available
        if self._ast_node:
            self._parse_from_ast()
        elif args:
            self._parse_from_args(args)

    def _parse_from_ast(self) -> None:
        """Parse from AST node."""
        if not self._ast_node:
            return

        from RDP.syntax_tree.nodes import (
            PositionalArgumentNode,
            KeywordArgumentNode,
            LiteralNode,
            IdentifierNode,
        )

        for arg in self._ast_node.arguments:
            if isinstance(arg, PositionalArgumentNode):
                # Positional argument is the group field
                if isinstance(arg.value, IdentifierNode):
                    self.group_field = arg.value.name
                elif isinstance(arg.value, LiteralNode):
                    self.group_field = str(arg.value.value)
            elif isinstance(arg, KeywordArgumentNode):
                if isinstance(arg.value, LiteralNode):
                    val = str(arg.value.value)
                elif isinstance(arg.value, IdentifierNode):
                    val = arg.value.name
                else:
                    val = str(arg.value)

                if arg.key == "maxspan":
                    self.maxspan = val
                elif arg.key == "field" or arg.key == "group_field":
                    self.group_field = val
                elif arg.key == "time_field":
                    self.time_field = val

    def _parse_from_args(self, args: list[str]) -> None:
        """Parse from args list."""
        for arg in args:
            if arg.startswith("maxspan="):
                self.maxspan = arg[8:].strip('"\'')
            elif arg.startswith("time_field="):
                self.time_field = arg[11:].strip('"\'')
            elif arg.startswith("field=") or arg.startswith("group_field="):
                self.group_field = arg.split("=", 1)[1].strip('"\'')
            elif "=" not in arg:
                # Assume it's the group field
                self.group_field = arg.strip('"\'')

    def _parse_maxspan(self, maxspan: str) -> pd.Timedelta:
        """
        Parse maxspan string to pandas Timedelta.

        Args:
            maxspan: Time span string (e.g., "5m", "1h", "30s")

        Returns:
            pd.Timedelta object
        """
        match = re.match(r"^(\d+)([smhdw])$", maxspan.lower())
        if not match:
            raise ValueError(f"Invalid maxspan format: {maxspan}. Use formats like 30s, 5m, 1h, 1d")

        value = int(match.group(1))
        unit = match.group(2)

        unit_map = {
            "s": "seconds",
            "m": "minutes",
            "h": "hours",
            "d": "days",
            "w": "weeks",
        }

        return pd.Timedelta(**{unit_map[unit]: value})

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the transaction operation."""
        if df.empty:
            return df

        if not self.group_field:
            raise ValueError("Group field not specified for transaction command")

        if self.group_field not in df.columns:
            raise ValueError(f"Group field not found: {self.group_field}")

        if self.time_field not in df.columns:
            raise ValueError(f"Time field not found: {self.time_field}")

        result = df.copy()

        # Ensure time column is datetime
        if not pd.api.types.is_datetime64_any_dtype(result[self.time_field]):
            result[self.time_field] = pd.to_datetime(result[self.time_field])

        # Sort by group field and time
        result = result.sort_values([self.group_field, self.time_field])

        # Parse maxspan
        maxspan_td = self._parse_maxspan(self.maxspan)

        # Calculate time differences within each group
        result["_time_diff"] = result.groupby(self.group_field)[self.time_field].diff()

        # Mark transaction boundaries (where time diff exceeds maxspan or is NaT)
        result["_new_transaction"] = (
            result["_time_diff"].isna() | 
            (result["_time_diff"] > maxspan_td)
        )

        # Assign transaction IDs
        result["_transaction_id"] = result.groupby(self.group_field)["_new_transaction"].cumsum()

        # Group by group_field and transaction_id to create transactions
        transactions = []
        
        for (group_val, trans_id), group_df in result.groupby([self.group_field, "_transaction_id"]):
            # Calculate transaction properties
            start_time = group_df[self.time_field].min()
            end_time = group_df[self.time_field].max()
            duration = (end_time - start_time).total_seconds()
            event_count = len(group_df)

            # Create transaction record with aggregated info
            trans_record = {
                self.group_field: group_val,
                self.time_field: start_time,
                "_end_time": end_time,
                "duration": duration,
                "event_count": event_count,
            }

            # Include first row's other columns (excluding internal columns)
            first_row = group_df.iloc[0]
            for col in df.columns:
                if col not in trans_record and col not in ["_time_diff", "_new_transaction", "_transaction_id"]:
                    trans_record[col] = first_row[col]

            transactions.append(trans_record)

        # Create result DataFrame
        if transactions:
            result_df = pd.DataFrame(transactions)
            # Ensure proper column order
            cols = [self.group_field, self.time_field, "_end_time", "duration", "event_count"]
            other_cols = [c for c in result_df.columns if c not in cols]
            result_df = result_df[cols + other_cols]
        else:
            result_df = pd.DataFrame()

        return result_df.reset_index(drop=True)

