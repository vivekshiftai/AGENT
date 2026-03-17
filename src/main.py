"""
PSBot entrypoint: FastAPI app with /chat, /plan, and test UI at /
Run: python src/main.py
Then open http://localhost:8000
"""
import sys
from pathlib import Path

# Ensure project root is on path when running as script
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.core.logging_config import setup_logging
from src.interfaces.api.routes import app

# UI is mounted in routes.py so it works for both main.py and uvicorn routes:app

if __name__ == "__main__":
    setup_logging()
    import uvicorn
    from src.core.config import settings
    port = settings.api_port
    print(f"Starting PSBot at http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
