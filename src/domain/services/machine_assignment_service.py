"""Machine assignment service for production planning."""
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.domain.entities.machine import Machine, AllergenCleaningTime
from src.domain.entities.production_task import ProductionTask, TaskType, TaskStatus
from src.domain.entities.recipe import Recipe
from src.domain.services.recipe_service import RecipeService

logger = logging.getLogger(__name__)


class MachineAssignmentService:
    """Service for assigning products to machines."""

    def __init__(
        self,
        machine_data: Optional[List[Dict[str, Any]]] = None,
        recipe_service: Optional[RecipeService] = None,
    ):
        self._machines: Dict[str, Machine] = {}
        self._recipes = recipe_service
        self._assignments: Dict[str, List[str]] = defaultdict(list)
        if machine_data:
            self.load_machines(machine_data)

    def load_machines(self, data: List[Dict[str, Any]]) -> None:
        """Load machines from normalized data records."""
        for record in data:
            machine = self._parse_machine(record)
            if machine:
                self._machines[machine.machine_id] = machine
        logger.info("MachineAssignmentService: loaded %d machines", len(self._machines))

    def _parse_machine(self, record: Dict[str, Any]) -> Optional[Machine]:
        """Parse a machine from a data record."""
        machine_id = str(record.get("machine_id") or record.get("equipment_id") or record.get("machine") or "")
        if not machine_id:
            return None
        
        allergen_cleaning = []
        raw_cleaning = record.get("allergen_cleaning_times") or record.get("cleaning_matrix") or {}
        if isinstance(raw_cleaning, dict):
            for from_allergen, to_times in raw_cleaning.items():
                if isinstance(to_times, dict):
                    for to_allergen, minutes in to_times.items():
                        allergen_cleaning.append(AllergenCleaningTime(
                            from_allergen=from_allergen,
                            to_allergen=to_allergen,
                            cleaning_minutes=float(minutes),
                        ))
                elif isinstance(to_times, (int, float)):
                    allergen_cleaning.append(AllergenCleaningTime(
                        from_allergen=from_allergen,
                        to_allergen="any",
                        cleaning_minutes=float(to_times),
                    ))
        
        return Machine(
            machine_id=machine_id,
            machine_name=str(record.get("machine_name") or record.get("name") or machine_id),
            machine_type=str(record.get("machine_type") or record.get("type") or "general"),
            plant_id=record.get("plant_id") or record.get("plant"),
            line_id=record.get("line_id") or record.get("line"),
            capacity_per_hour=float(record.get("capacity") or record.get("capacity_per_hour") or 100),
            capacity_unit=str(record.get("capacity_unit") or "EA"),
            available_hours_per_day=float(record.get("available_hours") or 8),
            compatible_products=record.get("compatible_products") or [],
            changeover_time_minutes=float(record.get("changeover_time") or record.get("changeover_minutes") or 30),
            default_cleaning_time_minutes=float(record.get("cleaning_time") or record.get("default_cleaning") or 60),
            allergen_cleaning_times=allergen_cleaning,
            status=str(record.get("status") or "available"),
        )

    def get_machine(self, machine_id: str) -> Optional[Machine]:
        """Get machine by ID."""
        return self._machines.get(machine_id)

    def get_all_machines(self) -> List[Machine]:
        """Get all loaded machines."""
        return list(self._machines.values())

    def get_compatible_machines(self, product_id: str) -> List[Machine]:
        """Get machines that can produce a specific product."""
        compatible = []
        for machine in self._machines.values():
            if machine.can_produce(product_id):
                compatible.append(machine)
        return compatible

    def assign_task_to_machine(
        self,
        task: ProductionTask,
        machine_id: Optional[str] = None,
    ) -> Optional[Machine]:
        """
        Assign a task to a machine. If machine_id not specified, find best available.
        """
        if machine_id:
            machine = self._machines.get(machine_id)
            if machine and machine.can_produce(task.product_id or ""):
                task.machine_id = machine.machine_id
                task.machine_name = machine.machine_name
                task.plant_id = machine.plant_id
                task.line_id = machine.line_id
                self._assignments[machine_id].append(task.task_id)
                return machine
            return None
        
        compatible = self.get_compatible_machines(task.product_id or "")
        if not compatible:
            compatible = list(self._machines.values())
        
        if not compatible:
            return None
        
        best_machine = min(
            compatible,
            key=lambda m: len(self._assignments.get(m.machine_id, []))
        )
        
        task.machine_id = best_machine.machine_id
        task.machine_name = best_machine.machine_name
        task.plant_id = best_machine.plant_id
        task.line_id = best_machine.line_id
        self._assignments[best_machine.machine_id].append(task.task_id)
        
        return best_machine

    def assign_tasks_to_machines(
        self,
        tasks: List[ProductionTask],
    ) -> Dict[str, List[ProductionTask]]:
        """
        Assign multiple tasks to machines.
        Returns dict of machine_id -> assigned tasks.
        """
        machine_tasks: Dict[str, List[ProductionTask]] = defaultdict(list)
        
        for task in tasks:
            if task.task_type != TaskType.PRODUCTION:
                continue
            
            machine = self.assign_task_to_machine(task)
            if machine:
                machine_tasks[machine.machine_id].append(task)
            else:
                logger.warning(
                    "MachineAssignmentService: no machine for task %s product %s",
                    task.task_id, task.product_id
                )
        
        return dict(machine_tasks)

    def get_tasks_for_machine(self, machine_id: str) -> List[str]:
        """Get task IDs assigned to a machine."""
        return self._assignments.get(machine_id, [])

    def calculate_cleaning_time(
        self,
        machine_id: str,
        from_product_allergens: List[str],
        to_product_allergens: List[str],
    ) -> float:
        """Calculate cleaning time when switching products on a machine."""
        machine = self._machines.get(machine_id)
        if not machine:
            return 60.0
        return machine.get_cleaning_time(from_product_allergens, to_product_allergens)

    def estimate_production_time(
        self,
        machine_id: str,
        quantity: float,
    ) -> float:
        """Estimate production time in minutes."""
        machine = self._machines.get(machine_id)
        if not machine:
            return quantity * 0.1
        return machine.estimate_production_time(quantity)

    def get_machine_workload(self) -> Dict[str, Dict[str, Any]]:
        """Get workload summary for all machines."""
        workload = {}
        for machine_id, task_ids in self._assignments.items():
            machine = self._machines.get(machine_id)
            workload[machine_id] = {
                "machine_id": machine_id,
                "machine_name": machine.machine_name if machine else machine_id,
                "task_count": len(task_ids),
                "task_ids": task_ids,
                "capacity_per_hour": machine.capacity_per_hour if machine else 0,
            }
        return workload

    def create_default_machine(self, machine_id: str) -> Machine:
        """Create a default machine when no machine data available."""
        machine = Machine(
            machine_id=machine_id,
            machine_name=f"Machine {machine_id}",
            machine_type="general",
            capacity_per_hour=100,
            available_hours_per_day=8,
            changeover_time_minutes=30,
            default_cleaning_time_minutes=60,
        )
        self._machines[machine_id] = machine
        return machine

    def ensure_machine_exists(self, machine_id: str) -> Machine:
        """Get or create a machine."""
        if machine_id in self._machines:
            return self._machines[machine_id]
        return self.create_default_machine(machine_id)

    def to_dict(self) -> Dict[str, Any]:
        """Export machines and assignments."""
        return {
            "machines": {k: v.to_dict() for k, v in self._machines.items()},
            "assignments": dict(self._assignments),
        }
