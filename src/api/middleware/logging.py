"""Request logging middleware.

Sets the request/query ID at request start using contextvars so the same ID
is used for the whole request lifecycle and appears on every log line. Uses
X-Request-Id or X-Query-Id from the client when present; otherwise generates
one. Clears the context in finally so IDs do not leak to other requests.
"""
import time
import logging
import uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

# Headers we accept for request/query ID (client can send one for end-to-end tracing)
REQUEST_ID_HEADERS = ("x-request-id", "x-query-id")

logger = logging.getLogger(__name__)


def _get_or_create_request_id(request: Request) -> str:
    """
    Use existing request ID from headers if present; otherwise generate one.
    We do not generate a new ID when the client already sent one (requirement: use existing ID).
    """
    for header in REQUEST_ID_HEADERS:
        value = request.headers.get(header)
        if value and value.strip():
            return value.strip()
    return str(uuid.uuid4())


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that:
    1. Sets request_id in contextvars at request start (from header or generated once).
    2. Logs request and response.
    3. Clears request_id in finally so it does not leak to other requests.
    """

    async def dispatch(self, request: Request, call_next):
        # Set request/query ID for this request so all code (and logging) use the same ID
        try:
            from shared.request_context import set_request_id, clear_request_id, get_request_id
        except ImportError:
            from src.shared.request_context import set_request_id, clear_request_id, get_request_id

        request_id = _get_or_create_request_id(request)
        set_request_id(request_id)

        start_time = time.time()
        try:
            # Log request (request_id will appear automatically via logging filter)
            client_ip = request.client.host if request.client else None
            logger.info(f"Request received - {request.method} {request.url.path} from {client_ip}")

            response = await call_next(request)

            duration = time.time() - start_time
            logger.info(
                f"Request completed - {request.method} {request.url.path} - "
                f"Status: {response.status_code} - Duration: {round(duration * 1000, 2)}ms"
            )
            return response
        finally:
            # Clear context so this request's ID is not reused by other requests/tasks
            clear_request_id()

