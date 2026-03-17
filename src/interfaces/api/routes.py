"""FastAPI routes: /chat, /plan, /datasources, static UI at /."""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src.core.logging_config import setup_logging
from src.infrastructure.database.psdb import ensure_psdb_exists, init_psdb_schema
from src.interfaces.api.controller import (
    add_datasource as add_datasource_handler,
    chat as chat_handler,
    delete_datasource as delete_datasource_handler,
    list_datasources as list_datasources_handler,
    plan as plan_handler,
    test_datasource_connection as test_connection_handler,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="PSBot Production Planning API", version="1.0.0")


class ChatRequest(BaseModel):
    message: str
    user_id: Optional[str] = None
    datasource_ids: Optional[List[int]] = None


class ChatResponse(BaseModel):
    response: str
    plan_tasks: list = []
    sales_orders_by_material: list = []
    product_targets: list = []
    material_shortages: list = []
    machine_schedules: list = []
    validation_issues: list = []
    risk_level: str = "low"
    inventory_summary: Optional[Dict[str, Any]] = None
    scheduling_summary: Optional[Dict[str, Any]] = None
    # CHG-specific fields
    allergen_warnings: list = []
    scheduling_exceptions: list = []


class PlanRequest(BaseModel):
    plan_id: Optional[str] = None
    input_data: Optional[dict] = None
    use_frepple: bool = False


class PlanResponse(BaseModel):
    plan_id: str
    tasks: list
    error: Optional[str] = None


class DatasourceCreate(BaseModel):
    name: str
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    extra_config: Optional[dict] = None


class DatasourceTest(BaseModel):
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    extra_config: Optional[dict] = None


@app.on_event("startup")
def startup():
    setup_logging()
    if ensure_psdb_exists():
        if init_psdb_schema():
            logger.info("Platform database psdb ready")
    else:
        logger.info("Running without psdb (start PostgreSQL and restart to enable datasource management)")


@app.post("/chat", response_model=ChatResponse)
def post_chat(body: ChatRequest) -> ChatResponse:
    """User sends a message; returns AI response with comprehensive planning data."""
    try:
        result = chat_handler(
            user_message=body.message,
            user_id=body.user_id,
            datasource_ids=body.datasource_ids,
        )
        return ChatResponse(
            response=result.get("response", ""),
            plan_tasks=result.get("plan_tasks") or [],
            sales_orders_by_material=result.get("sales_orders_by_material") or [],
            product_targets=result.get("product_targets") or [],
            material_shortages=result.get("material_shortages") or [],
            machine_schedules=result.get("machine_schedules") or [],
            validation_issues=result.get("validation_issues") or [],
            risk_level=result.get("risk_level", "low"),
            inventory_summary=result.get("inventory_summary"),
            scheduling_summary=result.get("scheduling_summary"),
            allergen_warnings=result.get("allergen_warnings") or [],
            scheduling_exceptions=result.get("scheduling_exceptions") or [],
        )
    except Exception as e:
        logger.exception("POST /chat failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/plan", response_model=PlanResponse)
def post_plan(body: Optional[PlanRequest] = None) -> PlanResponse:
    """Returns production plan tasks for Gantt chart."""
    try:
        req = body or PlanRequest()
        result = plan_handler(
            plan_id=req.plan_id,
            input_data=req.input_data,
            use_frepple=req.use_frepple,
        )
        return PlanResponse(
            plan_id=result.get("plan_id", "plan-1"),
            tasks=result.get("tasks") or [],
            error=result.get("error"),
        )
    except Exception as e:
        logger.exception("POST /plan failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/datasources")
def post_datasources(body: DatasourceCreate):
    """
    Add a new datasource. Tests connection first; if success saves and returns 200.
    If connection fails returns 400.
    """
    result = add_datasource_handler(
        name=body.name,
        type=body.type,
        host=body.host,
        port=body.port,
        database=body.database,
        username=body.username,
        password=body.password,
        extra_config=body.extra_config,
    )
    if result.get("success"):
        return result
    raise HTTPException(status_code=400, detail=result.get("error", "Connection failed"))


@app.get("/datasources")
def get_datasources() -> List[dict]:
    """Return all available datasources."""
    try:
        return list_datasources_handler()
    except Exception as e:
        logger.exception("GET /datasources failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/datasources/{id}")
def delete_datasources(id: int):
    """Remove datasource by id."""
    result = delete_datasource_handler(id)
    if result.get("success"):
        return result
    raise HTTPException(status_code=404, detail=result.get("error", "Not found"))


@app.post("/datasources/test")
def post_datasources_test(body: DatasourceTest):
    """Test connection without saving. Returns { success, error? }."""
    config = {
        "type": body.type,
        "host": body.host,
        "port": body.port,
        "database": body.database,
        "username": body.username,
        "password": body.password,
        **(body.extra_config or {}),
    }
    return test_connection_handler(config)


def mount_static(app: FastAPI, static_dir: Path) -> None:
    """Serve static files from static_dir (index.html, app.js, style.css)."""
    static_dir = Path(static_dir).resolve()

    @app.get("/")
    def serve_index():
        index = static_dir / "index.html"
        if not index.exists():
            raise HTTPException(status_code=404, detail="index.html not found")
        return FileResponse(index, media_type="text/html")

    @app.get("/app.js")
    def serve_app_js():
        f = static_dir / "app.js"
        if not f.is_file():
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(f, media_type="application/javascript")

    @app.get("/style.css")
    def serve_style_css():
        f = static_dir / "style.css"
        if not f.is_file():
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(f, media_type="text/css")


# Mount UI by default so it works with any entry point (main.py or uvicorn routes:app)
_ui_static_dir = Path(__file__).resolve().parent.parent / "web" / "static"
if _ui_static_dir.exists():
    mount_static(app, _ui_static_dir)
    logger.info("UI static files mounted at / (index.html, app.js, style.css)")
