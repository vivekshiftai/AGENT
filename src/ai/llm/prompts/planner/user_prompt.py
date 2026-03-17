"""User prompt template for product priority ordering."""
PLANNER_USER_TEMPLATE = """\
User request: {user_query}

Planning date range: {date_range}

PRODUCTION DATA:
{production_summary}

Return the optimal product processing order per machine as JSON. No markdown."""


def build_planner_user_prompt(
    user_query: str,
    date_range: str,
    product_summary: str,
) -> str:
    """
    Build the user prompt for product priority ordering.
    """
    return PLANNER_USER_TEMPLATE.format(
        user_query=user_query or "Optimize production sequence",
        date_range=date_range or "Not specified",
        production_summary=product_summary or "No data",
    )
