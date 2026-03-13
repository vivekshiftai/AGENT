"""Analytical Schema Preparation Node - Independent metadata extraction for the analytical flow.

This node parses SAP metadata XML independently from any existing schema processing logic
and extracts analytical schema information including dimensions, measures, data types, and
column labels. The resulting analytical schema is stored in dedicated state fields used
only by the new analytical flow.

Detection strategy:
1. Parse raw metadata XML from SAP Datasphere $metadata endpoint
2. Extract annotations (sap:aggregation-role, Analytics.Dimension/Measure, Common.Label)
3. Fall back to heuristic detection based on OData data types and naming conventions
4. Map each column with: internal name, analytical role, data type, user-friendly label

This module does NOT execute SAP fetches or construct API calls for data retrieval.
"""
import xml.etree.ElementTree as ET
import httpx
import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from ..state import AnalyticsState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SAP OData annotation constants
# ---------------------------------------------------------------------------

# XML namespaces used in OData CSDL metadata
ODATA_NAMESPACES = {
    "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
    "edm": "http://docs.oasis-open.org/odata/ns/edm",
    "sap": "http://www.sap.com/Protocols/SAPData",
}

# Annotation terms that indicate a dimension
DIMENSION_TERMS = {
    "Analytics.Dimension",
    "SAP__analytics.v1.Dimension",
    "com.sap.vocabularies.Analytics.v1.Dimension",
    "Aggregation.GroupableProperty",
}

# Annotation terms that indicate a measure
MEASURE_TERMS = {
    "Analytics.Measure",
    "SAP__analytics.v1.Measure",
    "com.sap.vocabularies.Analytics.v1.Measure",
    "Aggregation.CustomAggregate",
    "Aggregation.Aggregatable",
}

def _enrich_measure_descriptions(measures: List[Dict[str, Any]]) -> None:
    """
    Enrich measure entries with extra description for known SAP-derived measures.
    Mutates each matching measure dict in place.
    No-op when no known enrichments apply.
    """
    pass


# Annotation terms that carry a human-readable label
LABEL_TERMS = {
    "Common.Label",
    "SAP__common.v1.Label",
    "com.sap.vocabularies.Common.v1.Label",
}

# OData types typically associated with measures
NUMERIC_EDM_TYPES = {
    "Edm.Decimal", "Edm.Double", "Edm.Single",
    "Edm.Int16", "Edm.Int32", "Edm.Int64", "Edm.Byte", "Edm.SByte",
}

# OData types typically associated with time dimensions
DATE_EDM_TYPES = {
    "Edm.Date", "Edm.DateTimeOffset", "Edm.TimeOfDay",
}

# Common measure-like name patterns (case-insensitive)
MEASURE_NAME_PATTERNS = [
    r"(?i)(amount|quantity|qty|value|price|cost|revenue|total|sum|count|weight|volume|rate|ratio|percent|margin|profit|loss|budget|actual|variance|net|gross|discount|tax|freight|balance)",
]

# Columns to always exclude from analytical schema (technical/internal columns)
EXCLUDED_COLUMN_PATTERNS = [
    r"(?i)^__",          # Double-underscore technical columns
    r"(?i)^_sys_",       # System columns
    r"(?i)^metadata$",   # Metadata column
]


# ---------------------------------------------------------------------------
# Label generation helpers
# ---------------------------------------------------------------------------

def _generate_label_from_name(internal_name: str) -> str:
    """
    Generate a user-friendly label from an internal SAP column name.

    Transforms names like 'PLANT_CODE' -> 'Plant Code',
    'NetSalesAmount' -> 'Net Sales Amount', 'SD_DOC_TYPE' -> 'SD Doc Type'.

    Args:
        internal_name: Internal column name from SAP metadata

    Returns:
        Human-readable label string
    """
    if not internal_name:
        return internal_name

    # If already looks like a label (has spaces), return as-is with title case
    if " " in internal_name:
        return internal_name.strip().title()

    # Split on underscores first
    if "_" in internal_name:
        parts = internal_name.split("_")
        # Title-case each part
        return " ".join(p.capitalize() if p.isupper() or p.islower() else p for p in parts if p)

    # CamelCase splitting
    parts = re.sub(r"([a-z])([A-Z])", r"\1 \2", internal_name)
    parts = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", parts)
    return parts.strip().title()


def _is_excluded_column(name: str) -> bool:
    """Check if a column should be excluded from the analytical schema."""
    for pattern in EXCLUDED_COLUMN_PATTERNS:
        if re.match(pattern, name):
            return True
    return False


# ---------------------------------------------------------------------------
# Heuristic role detection
# ---------------------------------------------------------------------------

def _detect_role_by_heuristic(data_type: str, name: str) -> str:
    """
    Determine whether a column is a dimension or measure using heuristics.

    Strategy:
    1. Numeric OData types -> likely measure (unless name suggests otherwise)
    2. Date types -> dimension (time dimension)
    3. String/other types -> dimension
    4. Name pattern matching overrides type-based detection

    Args:
        data_type: OData type string (e.g. 'Edm.Decimal')
        name: Column name

    Returns:
        'dimension' or 'measure'
    """
    # Check name patterns first (they're often more reliable than type alone)
    name_upper = name.upper()
    for pattern in MEASURE_NAME_PATTERNS:
        if re.search(pattern, name):
            return "measure"

    # Date types are always dimensions (time dimensions)
    if data_type in DATE_EDM_TYPES:
        return "dimension"

    # Fiscal time columns (Edm.Int64 with fiscal-related names) are dimensions, not measures
    name_lower = name.lower()
    if data_type in NUMERIC_EDM_TYPES and any(kw in name_lower for kw in ("fiscal", "fiscper", "fisc")):
        return "dimension"

    # Numeric types default to measure
    if data_type in NUMERIC_EDM_TYPES:
        return "measure"

    # Everything else is a dimension
    return "dimension"


# ---------------------------------------------------------------------------
# XML annotation extraction
# ---------------------------------------------------------------------------

def _extract_annotations_from_xml(
    xml_content: str,
    view_name: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Parse SAP OData $metadata XML and extract dimensions and measures with labels.

    Supports three annotation patterns:
    1. SAP OData v2-style attributes:  sap:aggregation-role="dimension", sap:label="..."
    2. OData v4 inline annotations:    <Annotation Term="Analytics.Dimension"/> within <Property>
    3. OData v4 external annotations:  <Annotations Target="...EntityType/PropertyName">

    Args:
        xml_content: Raw XML string from $metadata endpoint
        view_name: Name of the SAP view being parsed

    Returns:
        Tuple of (dimensions_list, measures_list), each entry is a dict with
        {name, label, data_type, view_name, detection_method}
    """
    dimensions: List[Dict[str, Any]] = []
    measures: List[Dict[str, Any]] = []

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as exc:
        logger.warning(f"[prepare_analytical_schema] XML parse error for {view_name}: {exc}")
        return dimensions, measures

    # ------------------------------------------------------------------
    # Phase 1: Collect external annotations (Annotations Target="...")
    # ------------------------------------------------------------------
    external_annotations: Dict[str, Dict[str, Any]] = {}  # property_name -> {role, label}

    for annotations_elem in root.iter():
        tag = annotations_elem.tag
        # Match both namespaced and non-namespaced <Annotations> elements
        if not (tag.endswith("Annotations") and not tag.endswith("Annotation")):
            continue

        target = annotations_elem.get("Target", "")
        if "/" not in target:
            continue

        # Target format: "Namespace.EntityType/PropertyName"
        property_name = target.rsplit("/", 1)[-1]
        if not property_name:
            continue

        role = None
        label = None

        for annotation in annotations_elem:
            if not annotation.tag.endswith("Annotation"):
                continue
            term = annotation.get("Term", "")

            if term in DIMENSION_TERMS:
                role = "dimension"
            elif term in MEASURE_TERMS:
                role = "measure"
            elif term in LABEL_TERMS:
                label = annotation.get("String", "")

        if role or label:
            entry = external_annotations.setdefault(property_name, {})
            if role:
                entry["role"] = role
            if label:
                entry["label"] = label

    # ------------------------------------------------------------------
    # Phase 2: Walk EntityType → Property elements
    # ------------------------------------------------------------------
    entity_types_found = False

    for entity_type in root.iter():
        if not entity_type.tag.endswith("EntityType"):
            continue

        entity_name = entity_type.get("Name", "")
        # Match entity types related to our view
        if view_name not in entity_name and entity_name not in view_name:
            continue

        entity_types_found = True

        for prop in entity_type:
            if not prop.tag.endswith("Property"):
                continue

            col_name = prop.get("Name")
            if not col_name or _is_excluded_column(col_name):
                continue

            data_type = prop.get("Type", "Edm.String")

            # --- Detect role ---
            role = None
            label = None
            detection_method = "annotation"

            # Strategy 1: SAP v2-style attributes (sap:aggregation-role, sap:label)
            sap_role = prop.get(f"{{{ODATA_NAMESPACES['sap']}}}aggregation-role")
            if not sap_role:
                # Try un-namespaced attribute (some implementations use plain prefix)
                sap_role = prop.get("sap:aggregation-role")
            if sap_role:
                role = "dimension" if sap_role.lower() == "dimension" else "measure"

            sap_label = prop.get(f"{{{ODATA_NAMESPACES['sap']}}}label")
            if not sap_label:
                sap_label = prop.get("sap:label")
            if sap_label:
                label = sap_label

            # Strategy 2: OData v4 inline annotations within Property element
            for child in prop:
                if not child.tag.endswith("Annotation"):
                    continue
                term = child.get("Term", "")
                if term in DIMENSION_TERMS:
                    role = "dimension"
                elif term in MEASURE_TERMS:
                    role = "measure"
                elif term in LABEL_TERMS:
                    label = child.get("String", "")

            # Strategy 3: External annotations collected in Phase 1
            ext = external_annotations.get(col_name)
            if ext:
                if not role and "role" in ext:
                    role = ext["role"]
                if not label and "label" in ext:
                    label = ext["label"]

            # Strategy 4: Heuristic fallback
            if not role:
                role = _detect_role_by_heuristic(data_type, col_name)
                detection_method = "heuristic"

            # Generate label if not found from annotations
            if not label:
                label = _generate_label_from_name(col_name)

            column_entry = {
                "name": col_name,
                "label": label,
                "data_type": data_type,
                "view_name": view_name,
                "detection_method": detection_method,
            }

            if role == "measure":
                measures.append(column_entry)
            else:
                dimensions.append(column_entry)

    if not entity_types_found:
        logger.warning(
            f"[prepare_analytical_schema] No matching EntityType found for '{view_name}' in XML"
        )

    return dimensions, measures


def _extract_from_schema_heuristic(
    view_schema: Dict[str, Any],
    view_name: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fallback: extract dimensions and measures from an already-parsed schema using heuristics.

    Used when raw metadata XML is unavailable and we only have the basic column list
    from sap_view_schemas in state.

    Args:
        view_schema: Schema dict with 'columns' list of {name, type, max_length}
        view_name: Name of the SAP view

    Returns:
        Tuple of (dimensions_list, measures_list)
    """
    dimensions: List[Dict[str, Any]] = []
    measures: List[Dict[str, Any]] = []

    columns = view_schema.get("columns", [])

    for col in columns:
        col_name = col.get("name", "")
        data_type = col.get("type", "Edm.String")

        if not col_name or _is_excluded_column(col_name):
            continue

        role = _detect_role_by_heuristic(data_type, col_name)
        label = _generate_label_from_name(col_name)

        entry = {
            "name": col_name,
            "label": label,
            "data_type": data_type,
            "view_name": view_name,
            "detection_method": "heuristic",
        }

        if role == "measure":
            measures.append(entry)
        else:
            dimensions.append(entry)

    return dimensions, measures


# ---------------------------------------------------------------------------
# Raw metadata XML fetcher
# ---------------------------------------------------------------------------

async def _fetch_metadata_xml(metadata_url: str, token: str, timeout: float = 60.0) -> Optional[str]:
    """
    Fetch raw metadata XML from SAP Datasphere independently.

    Args:
        metadata_url: Full URL to the $metadata endpoint
        token: SAP Datasphere access token
        timeout: HTTP request timeout in seconds

    Returns:
        Raw XML string or None if fetch fails
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/xml",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            response = await client.get(metadata_url, headers=headers)

            if response.status_code == 200:
                return response.text
            else:
                logger.warning(
                    f"[prepare_analytical_schema] Metadata fetch returned {response.status_code} "
                    f"for URL: {metadata_url}"
                )
                return None
    except httpx.RequestError as exc:
        logger.warning(f"[prepare_analytical_schema] Metadata fetch failed: {exc}")
        return None


def _build_metadata_url(asset_info: Dict[str, Any], view_name: str) -> Optional[str]:
    """
    Build a metadata URL for a view from its asset information.

    Prefers the metadata_url from the catalog. If unavailable, attempts to
    construct one from the data_url by replacing the data path with $metadata.

    Args:
        asset_info: Asset dictionary from sap_datasphere_assets
        view_name: View name

    Returns:
        Full metadata URL or None
    """
    # Direct metadata URL from catalog (most reliable)
    metadata_url = asset_info.get("metadata_url")
    if metadata_url:
        # Ensure it ends with $metadata
        if not metadata_url.rstrip("/").endswith("$metadata"):
            metadata_url = metadata_url.rstrip("/") + "/$metadata"
        return metadata_url

    # Try constructing from data URL
    data_url = asset_info.get("data_url")
    if data_url:
        # Replace the data endpoint path with $metadata
        # Typical pattern: .../consumption/relational/{space}/{view} -> .../consumption/relational/{space}/{view}/$metadata
        base = data_url.rstrip("/")
        return f"{base}/$metadata"

    return None


# ---------------------------------------------------------------------------
# Main node function
# ---------------------------------------------------------------------------

async def prepare_analytical_schema_node(state: AnalyticsState) -> Dict[str, Any]:
    """
    Parse SAP metadata XML and extract analytical schema (dimensions, measures, labels).

    This node operates independently from existing schema processing logic (get_schema).
    It fetches raw metadata XML for each selected SAP view, parses it to extract
    analytical annotations, and stores the results in dedicated analytical state fields.

    For non-SAP data sources, this node is a no-op and returns an empty dict.

    Args:
        state: Current analytics state containing:
            - data_source_config: Data source configuration
            - selected_tables: Selected SAP views
            - sap_datasphere_assets: Catalog assets with metadata URLs
            - sap_access_token: OAuth token for SAP API calls
            - sap_view_schemas: Fallback parsed schemas (if XML fetch fails)

    Returns:
        Updated state dictionary with:
            - analytical_dimensions: List of dimension column descriptors
            - analytical_measures: List of measure column descriptors
    """
    start_time = datetime.now()
    node_name = "prepare_analytical_schema"

    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)

    logger.info(f"[{node_name}] Starting analytical schema preparation")

    # ---------------------------------------------------------------
    # Gate: Only process SAP Datasphere data sources
    # ---------------------------------------------------------------
    data_source_config = state.get("data_source_config", {})
    data_source_type = (data_source_config.get("type", "") if data_source_config else "").lower()

    if data_source_type not in ("sap", "sap_datasphere"):
        logger.info(f"[{node_name}] Non-SAP data source ({data_source_type}) - skipping")
        return {}

    selected_tables = state.get("selected_tables", [])
    sap_assets = state.get("sap_datasphere_assets", {})
    token = state.get("sap_access_token")

    if not selected_tables:
        logger.warning(f"[{node_name}] No selected tables - skipping")
        return {}

    if not token:
        logger.warning(f"[{node_name}] No SAP access token available - skipping")
        return {}

    # ---------------------------------------------------------------
    # Fetch and parse metadata XML for each selected view
    # ---------------------------------------------------------------
    all_dimensions: List[Dict[str, Any]] = []
    all_measures: List[Dict[str, Any]] = []
    assets_dict = sap_assets.get("assets", {})

    for view_name in selected_tables:
        asset_info = assets_dict.get(view_name, {})
        metadata_url = _build_metadata_url(asset_info, view_name)

        dims: List[Dict[str, Any]] = []
        meas: List[Dict[str, Any]] = []

        if metadata_url:
            logger.info(f"[{node_name}] Fetching metadata XML for view '{view_name}'")
            xml_content = await _fetch_metadata_xml(metadata_url, token)

            if xml_content:
                dims, meas = _extract_annotations_from_xml(xml_content, view_name)
                annotation_dims = sum(1 for d in dims if d.get("detection_method") == "annotation")
                annotation_meas = sum(1 for m in meas if m.get("detection_method") == "annotation")
                logger.info(
                    f"[{node_name}] View '{view_name}': "
                    f"{len(dims)} dimensions ({annotation_dims} annotated), "
                    f"{len(meas)} measures ({annotation_meas} annotated)"
                )
            else:
                logger.warning(
                    f"[{node_name}] XML fetch failed for '{view_name}' - falling back to heuristic"
                )
        else:
            logger.warning(
                f"[{node_name}] No metadata URL for '{view_name}' - falling back to heuristic"
            )

        # Heuristic fallback if XML-based extraction yielded nothing
        if not dims and not meas:
            schemas = state.get("sap_view_schemas", {})
            view_schema = schemas.get(view_name, {})
            if view_schema:
                dims, meas = _extract_from_schema_heuristic(view_schema, view_name)
                logger.info(
                    f"[{node_name}] Heuristic fallback for '{view_name}': "
                    f"{len(dims)} dimensions, {len(meas)} measures"
                )
            else:
                logger.warning(f"[{node_name}] No schema data available for '{view_name}'")

        all_dimensions.extend(dims)
        all_measures.extend(meas)

    # ---------------------------------------------------------------
    # Enrich known SAP-derived measures with calculation description (for LLM)
    # ---------------------------------------------------------------
    _enrich_measure_descriptions(all_measures)

    # ---------------------------------------------------------------
    # Enrich dimensions and measures with Excel column metadata (label, description)
    # so each column has user-facing label and description before column selection
    # and all downstream nodes (fetch plan, charts, summary) see them.
    # ---------------------------------------------------------------
    try:
        from ..utils.column_metadata_loader import load_column_metadata, enrich_columns_with_metadata
        column_metadata = load_column_metadata()
        if column_metadata:
            enrich_columns_with_metadata(all_dimensions, column_metadata)
            enrich_columns_with_metadata(all_measures, column_metadata)
            logger.info(
                f"[{node_name}] Enriched columns with Excel metadata for {len(column_metadata)} fields "
                "(label/short_text and description added to each matching column)"
            )
    except Exception as meta_err:
        logger.debug("[%s] Column metadata (Excel) load/enrich skipped: %s", node_name, meta_err)

    # ---------------------------------------------------------------
    # Summary logging
    # ---------------------------------------------------------------
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"[{node_name}] Analytical schema extraction complete | "
        f"Dimensions: {len(all_dimensions)} | Measures: {len(all_measures)} | "
        f"Views: {len(selected_tables)} | Duration: {duration:.2f}s"
    )

    if all_dimensions:
        dim_labels = [d["label"] for d in all_dimensions[:8]]
        logger.info(f"[{node_name}] Sample dimensions: {', '.join(dim_labels)}")
    if all_measures:
        meas_labels = [m["label"] for m in all_measures[:8]]
        logger.info(f"[{node_name}] Sample measures: {', '.join(meas_labels)}")

    return {
        "analytical_dimensions": all_dimensions,
        "analytical_measures": all_measures,
    }
