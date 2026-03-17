"""
Builds the per-machine scheduling user prompt sent to the LLM.
Extracted from LLMSchedulingNode so prompts are editable without touching
node logic.
"""
from typing import Any, Dict, List, Tuple


def build_machine_prompt(
    machine: Dict[str, Any],
    jobs: List[Dict[str, Any]],
    availability: List[Dict[str, Any]],
    allergen_rules: List[Dict[str, Any]],
    mrp_alerts: List[Dict[str, Any]],
    existing_wos: List[Dict[str, Any]],
) -> str:
    avail_lines = _format_availability(availability)
    job_lines = _format_jobs(jobs)
    rule_lines = _format_allergen_rules(allergen_rules)
    mrp_lines = _format_mrp_alerts(mrp_alerts)
    last_product, last_allergens = _get_last_product(existing_wos)

    clean_basic = machine.get("clean_basic_min", 40)

    return f"""
Machine: {machine.get('machine_name')} ({machine.get('machine_id')})
Plant: {machine.get('plant_id')} | Line: {machine.get('line_id', 'N/A')}
Capacity: {machine.get('cap_lbs_hr', '?')} lbs/hr | OEE: {machine.get('oee_pct', '?')}%
Setup time per new product: {machine.get('setup_time_min', 30)} min
Basic clean between same-allergen batches: {clean_basic} min
MTBF: {machine.get('mtbf_hrs', '?')} hrs

AVAILABLE SHIFTS THIS WEEK:
{avail_lines or '  No availability data -- assume standard 3-shift operation'}

LAST PRODUCT ON MACHINE: {last_product} | Allergens: {last_allergens}

JOB QUEUE ({len(jobs)} jobs -- sorted priority then due date):
{job_lines}

ALLERGEN CLEANING RULES FOR THIS MACHINE:
{rule_lines or f'  No specific rules -- use BASIC_CLEAN {clean_basic} min between all products'}

MATERIAL ALERTS (MRP):
{mrp_lines or '  No material shortages flagged for this machine'}

TASK:
Schedule all jobs for this week. Return ONLY a valid JSON array.

Rules you MUST follow:
1. PRIORITY: CRITICAL > HIGH > MEDIUM > LOW -- never delay CRITICAL for convenience
2. DATES: Never schedule a job so late it misses its required_by date
3. ALLERGEN CLEANING: Insert a cleaning block whenever allergen profile changes.
   Use the rules above. No matching rule = BASIC_CLEAN at {clean_basic} min.
4. ATP HOLD: After any allergen CIP where atp_swab_required, add a HOLD block
   for hold_min minutes before next production can start
5. GROUPING: Sequence same-allergen products together to minimize total clean time
6. CCP STEPS: Steps with qa_check_required=1 cannot be shortened or skipped
7. DOWNTIME: Never schedule production during MAINTENANCE/BREAKDOWN/CLEANING windows
8. MRP RED: Mark job as BLOCKED if ingredient shortage has no open PO coverage
9. SETUP: Add machine setup_time_min at start of each new product run
10. IQF PRE-COOL: For any FREEZING step, add a PRE_COOL block 90 min before
    the first frozen product of the day

Allowed block_type values and required fields:
- PRODUCTION: process_order_id, product_id, product_name, step_number, step_name,
              start_datetime, end_datetime, duration_min, allergens, priority,
              is_ccp (bool), batch_info, notes
- SETUP: product_id, product_name, start_datetime, end_datetime, duration_min, notes
- CLEANING: clean_type, from_product, from_product_name, from_allergens,
            to_product, to_product_name, to_allergens,
            start_datetime, end_datetime, duration_min, atp_swab_required (bool), notes
- HOLD: hold_reason, start_datetime, end_datetime, duration_min, notes
- PRE_COOL: start_datetime, end_datetime, duration_min, notes
- MAINTENANCE: start_datetime, end_datetime, duration_min, notes
- BLOCKED: process_order_id, product_id, product_name, blocked_reason,
           ingredient_blocked, unblock_date, notes
- EXCEPTION: exception_type, severity, process_order_id, notes
"""


# ── Private formatters ─────────────────────────────────────────────

def _format_availability(slots: List[Dict[str, Any]]) -> str:
    if not slots:
        return ""
    lines = []
    for s in slots:
        shift_start = str(s.get("shift_start", ""))[:16]
        shift_end = str(s.get("shift_end", ""))[:16]
        status = s.get("status", "")
        hrs = s.get("available_hrs", 0)
        reason = s.get("reason", "")
        lines.append(
            f"  {s.get('avail_date')} {s.get('shift', ''):<10} "
            f"{shift_start} \u2192 {shift_end} | "
            f"{hrs}h available | {status}"
            f"{' | ' + reason if reason else ''}"
        )
    return "\n".join(lines)


def _format_jobs(jobs: List[Dict[str, Any]]) -> str:
    if not jobs:
        return "  (no jobs)"
    lines = []
    for i, j in enumerate(jobs[:40], 1):
        allergens = ",".join(j.get("allergens") or ["NONE"])
        wait = f" + {j.get('wait_after_min')}min wait" if j.get("wait_after_min", 0) else ""
        ccp = " [CCP]" if j.get("qa_check_required") else ""
        lines.append(
            f"  {i:>2}. [{j.get('priority', '?'):<8}] {j.get('process_order_id', '?')} | "
            f"{j.get('product_name', '?')} | Allergens: {allergens} | "
            f"Step {j.get('step_number')}: {j.get('step_name', '?')} | "
            f"{j.get('duration_min')}min{wait}{ccp} | "
            f"Due: {str(j.get('required_by', '?'))[:10]}"
        )
    if len(jobs) > 40:
        lines.append(f"  ... and {len(jobs) - 40} more jobs")
    return "\n".join(lines)


def _format_allergen_rules(rules: List[Dict[str, Any]]) -> str:
    if not rules:
        return ""
    lines = []
    for r in rules:
        atp = (
            f" | ATP swab \u2014 hold {r.get('hold_min', '?')}min"
            if r.get("atp_swab_required")
            else ""
        )
        lines.append(
            f"  {r.get('from_allergens', '?')} \u2192 {r.get('to_allergens', '?')}: "
            f"{r.get('clean_type', '?')} | {r.get('clean_duration_min', '?')}min"
            f"{atp} | {r.get('risk_level', '?')} risk"
        )
    return "\n".join(lines)


def _format_mrp_alerts(alerts: List[Dict[str, Any]]) -> str:
    if not alerts:
        return ""
    lines = []
    for m in alerts:
        lines.append(
            f"  [{m.get('risk_level', '?')}] {m.get('ingredient_name', '?')}: "
            f"need {m.get('required_qty_lbs', '?')} lbs, "
            f"have {m.get('stock_on_hand_lbs', '?')} lbs, "
            f"{m.get('in_transit_lbs', 0)} lbs inbound | "
            f"Action: {m.get('action_needed', '?')}"
        )
    return "\n".join(lines)


def _get_last_product(existing_wos: List[Dict[str, Any]]) -> Tuple[str, str]:
    if not existing_wos:
        return "NONE", "UNKNOWN"
    sorted_wos = sorted(
        existing_wos,
        key=lambda x: x.get("planned_start", ""),
        reverse=True,
    )
    last = sorted_wos[0]
    return last.get("product_name", "UNKNOWN"), last.get("allergens", "UNKNOWN")
