"""User prompt builder for query understanding."""

QUERY_UNDERSTANDING_USER = """\
User message:
{user_message}"""


def build_query_understanding_prompt(user_message: str) -> str:
    """
    Build the user prompt for query understanding.
    """
    return QUERY_UNDERSTANDING_USER.format(user_message=user_message or "")
