"""
Query Planner - Creates and optimizes execution plans.

This module transforms AST into execution plans that can
be optimized before execution.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from syntax_tree.nodes import CommandAST, PipeCommandNode
    from pipe.commands.base import PipeCommand


@dataclass
class ExecutionStep:
    """A single step in the execution plan."""

    command_name: str
    ast_node: "PipeCommandNode | None" = None
    command: "PipeCommand | None" = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionPlan:
    """
    Execution plan for a command pipeline.

    Contains the source specification and a list of execution steps.
    """

    source_type: str = ""
    source_name: str = ""
    source_params: dict[str, Any] = field(default_factory=dict)
    steps: list[ExecutionStep] = field(default_factory=list)

    def add_step(self, step: ExecutionStep) -> None:
        """Add a step to the plan."""
        self.steps.append(step)

    def insert_step(self, index: int, step: ExecutionStep) -> None:
        """Insert a step at the given index."""
        self.steps.insert(index, step)

    def remove_step(self, index: int) -> ExecutionStep:
        """Remove and return the step at the given index."""
        return self.steps.pop(index)


class QueryPlanner:
    """
    Creates execution plans from AST and applies optimizations.
    """

    def __init__(self):
        from planner.optimizers import FilterOptimizer, HeadOptimizer
        self.optimizers = [
            FilterOptimizer(),
            HeadOptimizer(),
        ]

    def create_plan(self, ast: "CommandAST") -> ExecutionPlan:
        """
        Create an execution plan from an AST.

        Args:
            ast: The parsed CommandAST

        Returns:
            An ExecutionPlan ready for optimization and execution
        """
        plan = ExecutionPlan()

        # Set source information
        if ast.source:
            plan.source_type = ast.source.source_type
            plan.source_name = ast.source.source_name
            plan.source_params = dict(ast.source.parameters)

        # Create steps from pipe chain
        for pipe_node in ast.pipe_chain:
            step = ExecutionStep(
                command_name=pipe_node.name,
                ast_node=pipe_node,
            )
            plan.add_step(step)

        return plan

    def optimize(self, plan: ExecutionPlan) -> ExecutionPlan:
        """
        Apply optimizations to the execution plan.

        Args:
            plan: The execution plan to optimize

        Returns:
            The optimized plan
        """
        result = plan
        for optimizer in self.optimizers:
            result = optimizer.optimize(result)
        return result

    def create_commands(self, plan: ExecutionPlan) -> list["PipeCommand"]:
        """
        Create PipeCommand instances from the execution plan.

        Args:
            plan: The execution plan

        Returns:
            List of PipeCommand instances ready for execution
        """
        from pipe.services import PipeCommandFactory

        commands = []
        for step in plan.steps:
            if step.command is not None:
                commands.append(step.command)
            elif step.ast_node is not None:
                command = PipeCommandFactory.create_from_node(step.ast_node)
                step.command = command
                commands.append(command)
            else:
                raise ValueError(f"Step has no command or AST node: {step}")

        return commands

