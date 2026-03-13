"""
Load column metadata from Excel/CSV for analytical column selection.

Files in data/column_metadata/ with headers Value Field, Description, Short Text
are used to add label (short text) and description to each matching dimension/measure
**before** column selection. The LLM then sees name | label | description when
choosing columns. Selected columns (filtered_analytical_dimensions / filtered_analytical_measures)
carry label and description through to fetch plan, charts, and summary nodes.
"""
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Default directory: analytics-backend/data/column_metadata (from .../src/infrastructure/langgraph/utils/)
_DEFAULT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data" / "column_metadata"

# Expected headers (case-insensitive)
_VALUE_FIELD_ALIASES = ("value field", "valuefield", "column", "name")
_DESCRIPTION_ALIASES = ("description", "desc")
_SHORT_TEXT_ALIASES = ("short text", "shorttext", "label", "short_text")


def _normalize_header(h: str) -> str:
    return (h or "").strip().lower().replace(" ", "").replace("_", "")


def _find_column_index(df_columns: list, aliases: tuple) -> Optional[int]:
    """Return index of first column whose normalized name matches any alias."""
    alias_norms = {_normalize_header(a) for a in aliases}
    for i, col in enumerate(df_columns):
        if _normalize_header(str(col)) in alias_norms:
            return i
    return None


def load_column_metadata(directory: Optional[Path] = None) -> Dict[str, Dict[str, str]]:
    """
    Load column metadata from the first Excel or CSV file in the given directory.

    Expected file headers (case-insensitive): Value Field, Description, Short Text.
    Returns a dict keyed by value_field (lowercased for case-insensitive lookup):
    value_field.lower() -> {"description": str, "short_text": str}.
    Empty dict if directory missing, no file found, or parse error.
    """
    import pandas as pd

    dir_path = Path(directory) if directory else _DEFAULT_DIR
    if not dir_path.is_dir():
        logger.debug("Column metadata directory not found: %s", dir_path)
        return {}

    # Prefer .xlsx then .csv (.xls requires xlrd)
    candidates: list = []
    for ext in (".xlsx", ".csv"):
        candidates.extend(sorted(dir_path.glob(f"*{ext}")))
    if not candidates:
        logger.debug("No Excel/CSV files in column metadata directory: %s", dir_path)
        return {}

    path = candidates[0]
    try:
        if path.suffix.lower() == ".xlsx":
            df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
        else:
            df = pd.read_csv(path, encoding="utf-8", on_bad_lines="skip")
    except Exception as e:
        logger.warning("Failed to read column metadata file %s: %s", path, e)
        return {}

    if df.empty or len(df.columns) < 2:
        return {}

    cols = list(df.columns)
    idx_value = _find_column_index(cols, _VALUE_FIELD_ALIASES)
    idx_desc = _find_column_index(cols, _DESCRIPTION_ALIASES)
    idx_short = _find_column_index(cols, _SHORT_TEXT_ALIASES)

    if idx_value is None:
        logger.warning(
            "Column metadata file %s has no 'Value Field' column (headers: %s). Skipping.",
            path.name, cols[:5],
        )
        return {}

    out: Dict[str, Dict[str, str]] = {}
    for _, row in df.iterrows():
        raw = row.iloc[idx_value]
        value_field = (str(raw).strip() if raw is not None and pd.notna(raw) else "").strip()
        if not value_field:
            continue
        desc = ""
        if idx_desc is not None:
            v = row.iloc[idx_desc]
            desc = (str(v).strip() if v is not None and pd.notna(v) else "") or ""
        short = ""
        if idx_short is not None:
            v = row.iloc[idx_short]
            short = (str(v).strip() if v is not None and pd.notna(v) else "") or ""
        # Key by lowercased value_field so enrich_columns_with_metadata can match case-insensitively
        out[value_field.lower()] = {"description": desc, "short_text": short}
    logger.info("Loaded column metadata for %s columns from %s", len(out), path.name)
    return out


def enrich_columns_with_metadata(
    columns: List[Dict[str, Any]],
    metadata: Dict[str, Dict[str, str]],
    name_key: str = "name",
) -> None:
    """
    Enrich column dicts in place: set label from metadata short_text (if present)
    and description from metadata description (if present).
    Matching is case-insensitive: column name is compared to metadata keys (stored as lowercased).
    """
    if not metadata:
        return
    for col in columns or []:
        if not isinstance(col, dict):
            continue
        name = (col.get(name_key) or "").strip()
        if not name:
            continue
        # Case-insensitive lookup (metadata is keyed by value_field.lower())
        key = name.lower()
        if key not in metadata:
            continue
        info = metadata[key]
        if info.get("short_text"):
            col["label"] = info["short_text"].strip()
        if info.get("description"):
            col["description"] = info["description"].strip()
