"""Production plan aggregate."""
from dataclasses import dataclass
from typing import List

from src.domain.entities.task import Task


@dataclass
class ProductionPlan:
    """Aggregate of planned tasks (schedule) for visualization and execution."""

    plan_id: str
    tasks: List[Task]
    metadata: dict = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

    def to_gantt_tasks(self) -> List[dict]:
        """Return tasks in Gantt-ready format."""
        return [t.to_gantt_dict() for t in self.tasks]
