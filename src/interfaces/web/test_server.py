"""
Development test server: runs FastAPI app with static UI.
Usage: python -m src.interfaces.web.test_server
Then open http://localhost:8000
"""
import sys
from pathlib import Path

# Ensure project root is on path
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.interfaces.api.routes import app
from src.core.logging_config import setup_logging

# Mount static files for test UI
_static_dir = Path(__file__).resolve().parent / "static"
if _static_dir.exists():
    from src.interfaces.api.routes import mount_static
    mount_static(app, _static_dir)

if __name__ == "__main__":
    setup_logging()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
