"""Validate the generated production schedule.
CHG enhancement: validates machine_schedules_raw blocks for missed dates, allergen gaps."""
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from src.domain.entities.production_task import ProductionTask, TaskType, TaskStatus

logger = logging.getLogger(__name__)


def _validate_chg_schedule(
    machine_schedules: Dict[str, list],
    process_orders: List[Dict],
    exceptions: List[Dict],
) -> List[Dict]:
    """CHG-specific validation on top of LLM schedule output."""
    issues = list(exceptions)

    # 1. Check all CRITICAL orders finish before required_by
    critical_pos = [po for po in process_orders if po.get("priority") == "CRITICAL"]
    for po in critical_pos:
        po_id = po["process_order_id"]
        required = po.get("required_by") or ""
        last_end = None
        for mid, schedule in machine_schedules.items():
            for block in schedule:
                if block.get("process_order_id") == po_id and block.get("block_type") == "PRODUCTION":
                    end = block.get("end_datetime", "")
                    if not last_end or end > last_end:
                        last_end = end
        if last_end and required and last_end[:10] > str(required)[:10]:
            issues.append({
                "type": "MISSED_DATE",
                "severity": "CRITICAL",
                "message": (
                    f"CRITICAL order {po_id} ({po.get('product_name')}) "
                    f"finishes {last_end[:10]} but required by {str(required)[:10]}"
                ),
                "process_order_id": po_id,
            })

    # 2. Check non-critical orders for date risk
    for po in process_orders:
        if po.get("priority") == "CRITICAL":
            continue
        po_id = po["process_order_id"]
        required = po.get("required_by") or ""
        last_end = None
        for mid, schedule in machine_schedules.items():
            for block in schedule:
                if block.get("process_order_id") == po_id and block.get("block_type") == "PRODUCTION":
                    end = block.get("end_datetime", "")
                    if not last_end or end > last_end:
                        last_end = end
        if last_end and required and last_end[:10] > str(required)[:10]:
            issues.append({
                "type": "MISSED_DATE_RISK",
                "severity": "HIGH",
                "message": (
                    f"Order {po_id} ({po.get('product_name')}) "
                    f"finishes {last_end[:10]} but required by {str(required)[:10]}"
                ),
                "process_order_id": po_id,
            })

    # 3. Check allergen transitions have cleaning blocks
    for mid, schedule in machine_schedules.items():
        sorted_blocks = sorted(schedule, key=lambda x: x.get("start_datetime", ""))
        last_production_allergens = None
        last_was_cleaning = False
        for block in sorted_blocks:
            bt = block.get("block_type", "")
            if bt == "CLEANING":
                last_was_cleaning = True
                continue
            if bt == "PRODUCTION":
                curr_allergens = set(block.get("allergens") or [])
                if last_production_allergens is not None and curr_allergens:
                    removed = last_production_allergens - curr_allergens
                    if removed and not last_was_cleaning:
                        issues.append({
                            "type": "MISSING_ALLERGEN_CLEAN",
                            "severity": "CRITICAL",
                            "message": (
                                f"Machine {mid}: allergen {removed} removed between products "
                                f"without a cleaning block before {block.get('product_name', '?')}"
                            ),
                            "machine_id": mid,
                            "process_order_id": block.get("process_order_id"),
                        })
                last_production_allergens = curr_allergens
                last_was_cleaning = False

    # 4. Count blocked POs
    blocked_count = 0
    for mid, schedule in machine_schedules.items():
        for block in schedule:
            if block.get("block_type") == "BLOCKED":
                blocked_count += 1

    return issues


def run(state: Dict[str, Any]) -> Dict[str, Any]:
    if state.get("needsClarification") or state.get("rejected"):
        return {**state}

    is_chg = state.get("_is_chg", False)

    # ── CHG path ────────────────────────────────────────────────────
    if is_chg:
        machine_schedules = state.get("machine_schedules_raw") or {}
        process_orders = state.get("process_orders") or []
        exceptions = state.get("scheduling_exceptions") or []

        all_issues = _validate_chg_schedule(machine_schedules, process_orders, exceptions)

        critical_count = sum(1 for i in all_issues if i.get("severity") == "CRITICAL")
        high_count = sum(1 for i in all_issues if i.get("severity") == "HIGH")

        total_blocks = sum(len(s) for s in machine_schedules.values())
        production_blocks = sum(
            len([b for b in s if b.get("block_type") == "PRODUCTION"])
            for s in machine_schedules.values()
        )
        blocked_blocks = sum(
            len([b for b in s if b.get("block_type") == "BLOCKED"])
            for s in machine_schedules.values()
        )

        validation_result = {
            "valid": critical_count == 0,
            "issues": all_issues,
            "warnings": [],
            "summary": {
                "total_process_orders": len(process_orders),
                "total_blocks": total_blocks,
                "production_blocks": production_blocks,
                "blocked_blocks": blocked_blocks,
                "critical_issues": critical_count,
                "high_issues": high_count,
                "machines_scheduled": len(machine_schedules),
            },
        }

        logger.info(
            "ScheduleValidationNode CHG: valid=%s, %d issues, %d critical, %d high",
            validation_result["valid"], len(all_issues), critical_count, high_count,
        )

        return {
            **state,
            "validationResult": validation_result,
            "scheduling_exceptions": all_issues,
        }

    # ── Generic path (unchanged) ────────────────────────────────────
    production_tasks = state.get("_production_tasks") or []
    scheduling_result = state.get("schedulingResult") or {}
    inventory_check = state.get("inventoryCheck") or {}
    machines_dict = state.get("machines") or {}
    date_range = state.get("dateRange") or {}

    validation_result = {
        "valid": True,
        "issues": [],
        "warnings": [],
        "summary": {
            "total_tasks": len(production_tasks),
            "validated_tasks": 0,
            "at_risk_tasks": 0,
            "blocked_tasks": 0,
        },
    }

    task_by_id = {t.task_id: t for t in production_tasks}
    today = date.today()
    planning_start = today
    planning_end = today + timedelta(days=30)
    if date_range:
        try:
            if date_range.get("start"):
                planning_start = date.fromisoformat(str(date_range["start"])[:10])
            if date_range.get("end"):
                planning_end = date.fromisoformat(str(date_range["end"])[:10])
        except (ValueError, TypeError):
            pass

    for machine_id, schedule in scheduling_result.get("machines", {}).items():
        task_sequence = schedule.get("task_sequence", [])
        machine_data = machines_dict.get(machine_id, {})
        capacity_per_hour = machine_data.get("capacity_per_hour", 100)
        available_hours = machine_data.get("available_hours_per_day", 8)
        current_time = datetime.combine(planning_start, datetime.min.time())
        for task_id in task_sequence:
            task = task_by_id.get(task_id)
            if not task:
                continue
            production_hours = task.quantity / capacity_per_hour if capacity_per_hour > 0 else 24
            production_days = max(1, int(production_hours / available_hours) + 1)
            task.scheduled_start = current_time
            task.scheduled_end = current_time + timedelta(days=production_days)
            if task.delivery_target_date:
                if task.scheduled_end.date() > task.delivery_target_date:
                    task.risk_level = "high"
                    task.risk_notes = f"Scheduled completion {task.scheduled_end.date()} after delivery {task.delivery_target_date}"
                    validation_result["issues"].append({
                        "type": "delivery_risk",
                        "task_id": task_id,
                        "product": task.product_name,
                        "delivery_date": task.delivery_target_date.isoformat(),
                        "scheduled_end": task.scheduled_end.date().isoformat(),
                        "delay_days": (task.scheduled_end.date() - task.delivery_target_date).days,
                    })
                    validation_result["summary"]["at_risk_tasks"] += 1
                elif (task.delivery_target_date - task.scheduled_end.date()).days <= 1:
                    task.risk_level = "medium"
                    task.risk_notes = "Tight delivery timeline"
                    validation_result["warnings"].append({
                        "type": "tight_timeline",
                        "task_id": task_id,
                        "product": task.product_name,
                        "buffer_days": (task.delivery_target_date - task.scheduled_end.date()).days,
                    })
                else:
                    task.risk_level = "low"
            current_time = task.scheduled_end
            for ce in schedule.get("cleaning_events", []):
                if ce.get("before_task") == task_id:
                    current_time += timedelta(minutes=ce.get("cleaning_minutes", 60))
            validation_result["summary"]["validated_tasks"] += 1

    shortages = inventory_check.get("shortages", [])
    for shortage in shortages:
        task_id = None
        for task in production_tasks:
            if task.product_id == shortage.get("product_id"):
                task_id = task.task_id
                task.status = TaskStatus.BLOCKED
                task.risk_level = "high"
                task.risk_notes = f"Material shortage: {shortage.get('material_id')}"
                validation_result["summary"]["blocked_tasks"] += 1
                break
        validation_result["issues"].append({
            "type": "material_shortage",
            "task_id": task_id,
            "product_id": shortage.get("product_id"),
            "material_id": shortage.get("material_id"),
            "required": shortage.get("required"),
            "available": shortage.get("available"),
            "shortage": shortage.get("shortage"),
        })

    for machine_id, machine_data in machines_dict.items():
        task_ids = scheduling_result.get("machines", {}).get(machine_id, {}).get("task_sequence", [])
        total_quantity = sum(task_by_id[tid].quantity for tid in task_ids if tid in task_by_id)
        capacity_per_hour = machine_data.get("capacity_per_hour", 100)
        available_hours = machine_data.get("available_hours_per_day", 8)
        planning_days = (planning_end - planning_start).days or 1
        total_capacity = capacity_per_hour * available_hours * planning_days
        utilization = (total_quantity / total_capacity * 100) if total_capacity > 0 else 0
        if utilization > 100:
            validation_result["issues"].append({
                "type": "capacity_exceeded",
                "machine_id": machine_id,
                "utilization_percent": round(utilization, 1),
                "total_quantity": total_quantity,
                "total_capacity": total_capacity,
            })
        elif utilization > 90:
            validation_result["warnings"].append({
                "type": "high_utilization",
                "machine_id": machine_id,
                "utilization_percent": round(utilization, 1),
            })

    if validation_result["issues"]:
        validation_result["valid"] = False

    production_tasks_dicts = [t.to_dict() for t in production_tasks]

    logger.info(
        "ScheduleValidationNode: valid=%s, %d issues, %d warnings, %d at-risk, %d blocked",
        validation_result["valid"], len(validation_result["issues"]),
        len(validation_result["warnings"]),
        validation_result["summary"]["at_risk_tasks"],
        validation_result["summary"]["blocked_tasks"],
    )

    return {
        **state,
        "validationResult": validation_result,
        "productionTasks": production_tasks_dicts,
        "_production_tasks": production_tasks,
    }
