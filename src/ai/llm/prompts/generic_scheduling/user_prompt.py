"""User prompt templates for generic LLM scheduling."""

SCHEDULING_OPTIMIZER_USER_TEMPLATE = """\
USER REQUEST: {user_query}

PLANNING PERIOD: {date_range}

MACHINE: {machine_id}
Machine Type: {machine_type}
Capacity: {capacity_per_hour} units/hour
Available Hours: {available_hours} hours/day
Default Changeover Time: {changeover_time} minutes
Default Cleaning Time: {default_cleaning_time} minutes

ALLERGEN CLEANING TIMES:
{allergen_cleaning_times}

TASKS TO SCHEDULE:
{tasks_json}

OPTIMIZATION OBJECTIVES:
1. Maximize production output within available time
2. Meet all delivery target dates (flag any at-risk tasks)
3. Minimize allergen-related cleaning downtime by grouping similar allergen products
4. Optimize product sequencing to reduce changeover time
5. Group products with same allergens together to minimize contamination risk

Provide the optimal task sequence as JSON with: task_sequence, reasoning, \
estimated_total_time_minutes, cleaning_events, risk_assessment."""

MULTI_MACHINE_USER_TEMPLATE = """\
USER REQUEST: {user_query}

PLANNING PERIOD: {date_range}

AVAILABLE MACHINES:
{machines_json}

ALL TASKS TO SCHEDULE:
{tasks_json}

MACHINE CONSTRAINTS:
{constraints_json}

OPTIMIZATION GOALS:
- maximize_production: {maximize_production}
- minimize_delivery_delays: {minimize_delivery_delays}
- minimize_changeover_time: {minimize_changeover_time}
- balance_workload: {balance_workload}

Provide: optimal task assignment, sequence per machine, cleaning/changeover schedule, \
risk assessment. Return valid JSON only."""


def build_scheduling_user_prompt(
    user_query: str,
    date_range: str,
    machine_id: str,
    machine_type: str,
    capacity_per_hour: float,
    available_hours: float,
    changeover_time: float,
    default_cleaning_time: float,
    allergen_cleaning_times: str,
    tasks_json: str,
) -> str:
    """
    Build the user prompt for single-machine scheduling optimization.
    """
    return SCHEDULING_OPTIMIZER_USER_TEMPLATE.format(
        user_query=user_query or "Optimize production schedule",
        date_range=date_range or "Not specified",
        machine_id=machine_id,
        machine_type=machine_type or "general",
        capacity_per_hour=capacity_per_hour,
        available_hours=available_hours,
        changeover_time=changeover_time,
        default_cleaning_time=default_cleaning_time,
        allergen_cleaning_times=allergen_cleaning_times or "No specific allergen cleaning times defined",
        tasks_json=tasks_json,
    )


def build_multi_machine_prompt(
    user_query: str,
    date_range: str,
    machines_json: str,
    tasks_json: str,
    constraints_json: str,
    maximize_production: bool = True,
    minimize_delivery_delays: bool = True,
    minimize_changeover_time: bool = True,
    balance_workload: bool = True,
) -> str:
    """
    Build the user prompt for multi-machine scheduling optimization.
    """
    return MULTI_MACHINE_USER_TEMPLATE.format(
        user_query=user_query or "Optimize production schedule",
        date_range=date_range or "Not specified",
        machines_json=machines_json,
        tasks_json=tasks_json,
        constraints_json=constraints_json,
        maximize_production=maximize_production,
        minimize_delivery_delays=minimize_delivery_delays,
        minimize_changeover_time=minimize_changeover_time,
        balance_workload=balance_workload,
    )
