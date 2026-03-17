"""Task entity for production scheduling and Gantt visualization."""
from dataclasses import dataclass
from datetime import date
from typing import List, Optional


@dataclass
class Task:
    """A single production or planning task with timing and dependencies."""

    id: str
    name: str
    start: date
    end: date
    progress: int = 0
    dependencies: Optional[List[str]] = None

    def to_gantt_dict(self) -> dict:
        """Format for Frappe Gantt: id, name, start, end, progress, dependencies."""
        out = {
            "id": self.id,
            "name": self.name,
            "start": self.start.isoformat() if hasattr(self.start, "isoformat") else str(self.start),
            "end": self.end.isoformat() if hasattr(self.end, "isoformat") else str(self.end),
            "progress": self.progress,
        }
        if self.dependencies:
            out["dependencies"] = self.dependencies
        return out
