"""
frePPLe C++ engine runner - invokes the frePPLe executable via subprocess.

Runs the frePPLe planning engine with JSON input and captures the plan output.
"""
import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

FREPPLE_SEARCH_PATHS = [
    Path(__file__).parent.parent.parent / "bin" / "frepple",
    Path(__file__).parent.parent.parent / "bin" / "frepple.exe",
    Path(__file__).parent.parent.parent.parent / "bin" / "frepple",
    Path(__file__).parent.parent.parent.parent / "bin" / "frepple.exe",
    Path.cwd() / "bin" / "frepple",
    Path.cwd() / "bin" / "frepple.exe",
]


def _find_frepple_executable(custom_path: Optional[str] = None) -> Optional[Path]:
    """Locate the frePPLe executable."""
    if custom_path:
        p = Path(custom_path)
        if p.exists():
            return p
        p_exe = Path(str(p) + ".exe")
        if p_exe.exists():
            return p_exe
        return None
    for p in FREPPLE_SEARCH_PATHS:
        if p.exists():
            return p
    return None


def run_frepple(
    input_json: Dict[str, Any],
    output_path: Optional[Path] = None,
    frepple_path: Optional[str] = None,
    timeout_seconds: int = 300,
) -> Dict[str, Any]:
    """
    Run frePPLe with JSON input and return the plan output.

    Args:
        input_json: frePPLe plan model as dict (items, operations, demands, buffers, etc.)
        output_path: Optional path to write plan output JSON. If None, uses temp file.
        frepple_path: Optional path to frePPLe executable.
        timeout_seconds: Subprocess timeout.

    Returns:
        Parsed plan output (operationplans, flowplans, loadplans, etc.) or empty dict on error.
    """
    exe = _find_frepple_executable(frepple_path)
    if not exe:
        logger.error(
            "frePPLe executable not found. Build the C++ project first (cmake, make) "
            "or set FREPPLE_PATH environment variable."
        )
        return {}

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        json.dump(input_json, f, indent=2)
        input_file = f.name

    try:
        if "solvers" not in input_json and "solver" not in str(input_json):
            plan_data = input_json.get("plan", input_json)
            if isinstance(plan_data, dict):
                solvers = plan_data.get("solvers", [])
                if not solvers:
                    plan_data["solvers"] = [{"type": "solver_mrp", "create": True}]
            else:
                input_json["solvers"] = [{"type": "solver_mrp", "create": True}]

        out_path = output_path or tempfile.mktemp(suffix=".json")
        out_path = str(out_path)

        wrapper = _create_wrapper_script(input_file, out_path)
        wrapper_file = tempfile.mktemp(suffix=".py")
        with open(wrapper_file, "w", encoding="utf-8") as wf:
            wf.write(wrapper)

        try:
            cmd = [str(exe), wrapper_file]
            logger.info("Running frePPLe: %s", " ".join(cmd))
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                cwd=str(Path(input_file).parent),
                env={**os.environ, "FREPPLE_HOME": str(exe.parent.parent)},
            )
            if result.returncode != 0:
                logger.error(
                    "frePPLe failed (exit %d): %s",
                    result.returncode,
                    result.stderr or result.stdout,
                )
                return {}

            if Path(out_path).exists():
                with open(out_path, "r", encoding="utf-8") as of:
                    return json.load(of)
        finally:
            if Path(wrapper_file).exists():
                try:
                    os.unlink(wrapper_file)
                except OSError:
                    pass
    except subprocess.TimeoutExpired:
        logger.error("frePPLe timed out after %d seconds", timeout_seconds)
        return {}
    except json.JSONDecodeError as e:
        logger.error("Invalid frePPLe output JSON: %s", e)
        return {}
    except Exception as e:
        logger.exception("frePPLe execution failed: %s", e)
        return {}
    finally:
        if Path(input_file).exists():
            try:
                os.unlink(input_file)
            except OSError:
                pass

    return {}


def _create_wrapper_script(input_path: str, output_path: str) -> str:
    """Create a Python script that frePPLe can run: load JSON, solve, save plan."""
    return f'''
import frepple

frepple.readJSONfile("{input_path.replace(chr(92), "/")}")

solver = frepple.solver_mrp(name="MRP")
solver.solve()

frepple.saveJSONfile("{output_path.replace(chr(92), "/")}", "PLAN", 1)
'''
