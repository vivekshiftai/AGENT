"""Production planning domain service - builds plans from data or frePPLe."""
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

from src.domain.entities.production_plan import ProductionPlan
from src.domain.entities.task import Task

logger = logging.getLogger(__name__)


class ProductionPlanner:
    """
    Domain service that produces production plans (tasks) for visualization.

    Can delegate to frePPLe or generate mock/sample plans for testing.
    """

    def __init__(self, frepple_runner=None):
        self._frepple_runner = frepple_runner

    def create_plan(
        self,
        plan_id: str,
        input_data: Dict[str, Any] = None,
        use_frepple: bool = False,
    ) -> ProductionPlan:
        """
        Create a production plan. If use_frepple and runner available, run frePPLe;
        else if input_data has "records", build tasks from real data; else return mock for UI testing.
        """
        if use_frepple and self._frepple_runner:
            result = self._frepple_runner(input_data or {})
            tasks = self._tasks_from_frepple_result(result)
            logger.info("ProductionPlanner: frePPLe plan with %s tasks", len(tasks))
            return ProductionPlan(plan_id=plan_id, tasks=tasks, metadata={})

        records = (input_data or {}).get("records") if isinstance(input_data, dict) else []
        if records:
            tasks = self._tasks_from_records(records)
            logger.info("ProductionPlanner: plan from data with %s tasks (input records: %s)", len(tasks), len(records))
            return ProductionPlan(plan_id=plan_id, tasks=tasks, metadata={})

        tasks = self._mock_tasks(plan_id)
        logger.info("ProductionPlanner: no input data, returning mock plan with %s tasks", len(tasks))
        return ProductionPlan(plan_id=plan_id, tasks=tasks, metadata={})

    def _tasks_from_frepple_result(self, result: Dict[str, Any]) -> List[Task]:
        """Convert frePPLe plan output to Task list."""
        tasks = []
        op_plans = result.get("operationplans", result.get("operationplan", []))
        if not isinstance(op_plans, list):
            op_plans = [op_plans] if op_plans else []
        for i, op in enumerate(op_plans):
            if not isinstance(op, dict):
                continue
            start = op.get("start") or op.get("startdate")
            end = op.get("end") or op.get("enddate")
            if not start or not end:
                continue
            task_id = op.get("id") or op.get("name") or f"task_{i+1}"
            name = op.get("operation") or op.get("name") or task_id
            tasks.append(
                Task(
                    id=str(task_id),
                    name=str(name),
                    start=self._parse_date(start),
                    end=self._parse_date(end),
                    progress=0,
                    dependencies=op.get("dependencies"),
                )
            )
        return tasks

    def _parse_date(self, value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value.strip()[:10])
        raise ValueError(f"Cannot parse date: {value}")

    def _tasks_from_records(self, records: List[Dict[str, Any]]) -> List[Task]:
        """Build Gantt tasks from normalized records (source, product, quantity, date)."""
        tasks = []
        seen = set()
        for i, r in enumerate(records):
            if not isinstance(r, dict):
                continue
            d = r.get("date")
            if hasattr(d, "date"):
                d = d.date().isoformat()
            elif isinstance(d, str) and len(d) >= 10:
                d = d[:10]
            elif isinstance(d, (tuple, list)) and len(d) >= 3:
                d = "%s-%s-%s" % (d[0], d[1], d[2])[:10]
            else:
                d = date.today().isoformat()
            product = str(r.get("product") or r.get("item") or "")
            source = str(r.get("source") or "")
            key = (source, product, d)
            if key in seen:
                continue
            seen.add(key)
            try:
                start_d = self._parse_date(d)
                end_d = start_d + timedelta(days=1)
            except Exception:
                start_d = date.today()
                end_d = start_d + timedelta(days=1)
            task_id = ("task-%s-%s-%s" % (source, product, str(d)[:10])).replace(" ", "_")
            name = "%s @ %s" % (product or "Order", source) if source else (product or "Order")
            tasks.append(
                Task(
                    id=task_id[:64],
                    name=name[:80],
                    start=start_d,
                    end=end_d,
                    progress=0,
                )
            )
        return tasks

    def _mock_tasks(self, plan_id: str) -> List[Task]:
        """Generate a few sample tasks for Gantt testing."""
        base = date(2026, 3, 1)
        return [
            Task(
                id="task1",
                name="Production Batch A",
                start=base,
                end=base + timedelta(days=4),
                progress=20,
            ),
            Task(
                id="task2",
                name="Production Batch B",
                start=base + timedelta(days=2),
                end=base + timedelta(days=7),
                progress=0,
                dependencies=["task1"],
            ),
            Task(
                id="task3",
                name="Quality Check",
                start=base + timedelta(days=5),
                end=base + timedelta(days=6),
                progress=0,
                dependencies=["task2"],
            ),
        ]
