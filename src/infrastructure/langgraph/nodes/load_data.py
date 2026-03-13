"""Data loading node - loads and normalizes Excel/CSV data exactly once per query using LLM.

This node:
1. Loads Excel/CSV files once per query (raw data, no normalization)
2. Calls LLM to detect date columns and determine normalization method
3. Applies LLM-suggested normalization to convert dates to ISO format (datetime64[ns])
4. Stores normalized DataFrames in state["dataframes"]
5. All downstream nodes reuse these DataFrames
"""
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import json
import re

from ..state import AnalyticsState
from ...llm.azure_openai import AzureOpenAIClient
from ..prompts import (
    COLUMN_NORMALIZATION_SYSTEM_PROMPT,
    get_column_normalization_user_prompt
)
from ..utils import save_llm_call_input, save_llm_call_output
from ...database.data_source_gateway import read_excel_with_engine, read_csv_with_encoding, get_excel_file_engine
from shared.exceptions import DatabaseException
from config.settings import settings
from ...cache import get_cleaned_data_cache

logger = logging.getLogger(__name__)


def _smart_multi_format_date_parser(series: pd.Series, node_name: str) -> Optional[pd.Series]:
    """
    Smart fallback parser that tries multiple common date formats.
    Used when LLM-suggested method fails or cannot be parsed.
    """
    def _parse_value(val):
        """Try multiple date formats to parse a single value."""
        if pd.isna(val):
            return pd.NaT
        try:
            val_str = str(val).strip()
            
            # List of common date formats to try (in order of likelihood)
            date_formats = [
                '%Y-%m-%d',      # 2024-01-15
                '%Y/%m/%d',      # 2024/01/15
                '%d/%m/%Y',      # 15/01/2024
                '%m/%d/%Y',      # 01/15/2024
                '%d-%m-%Y',      # 15-01-2024
                '%m-%d-%Y',      # 01-15-2024
                '%Y-%m',         # 2024-01 (month/year)
                '%Y/%m',         # 2024/01 (month/year)
                '%m/%Y',         # 01/2024 (month/year)
                '%m-%Y',         # 01-2024 (month/year)
                '%B %Y',         # January 2024 (month name/year)
                '%b %Y',         # Jan 2024 (abbreviated month/year)
                '%Y%m%d',        # 20240115 (integer format)
            ]
            
            # Try each format
            for fmt in date_formats:
                try:
                    result = pd.to_datetime(val_str, format=fmt, errors="coerce")
                    if not pd.isna(result):
                        # If it's a month/year format, ensure it has day 1
                        if fmt in ['%Y-%m', '%Y/%m', '%m/%Y', '%m-%Y', '%B %Y', '%b %Y']:
                            if hasattr(result, 'replace'):
                                return result.replace(day=1)
                            return result
                        return result
                except (ValueError, TypeError):
                    continue
            
            # If all formats fail, try pandas' automatic parsing
            result = pd.to_datetime(val_str, errors="coerce", infer_datetime_format=True)
            if not pd.isna(result):
                return result
                
        except Exception:
            pass
        return pd.NaT
    
    try:
        parsed_series = series.apply(_parse_value)
        # Check if we got any valid dates
        if parsed_series.notna().sum() > 0:
            return parsed_series
        return None
    except Exception as e:
        logger.error(f"[{node_name}] Smart parser failed: {e}")
        return None


def _apply_cleaning_steps(series: pd.Series, cleaning_steps: List[str]) -> pd.Series:
    """
    Apply cleaning steps to a series based on LLM recommendations.
    
    Supported cleaning steps:
    - "remove_dollar_signs": Remove $ symbols
    - "remove_currency_symbols": Remove all currency symbols ($, €, £, ¥, ₹, etc.)
    - "remove_commas": Remove comma separators (thousands separator)
    - "remove_percent_signs": Remove % symbols
    - "replace_hash_with_zero": Replace # with 0 (for numeric columns)
    - "replace_hash_with_empty": Replace # with empty string (for text columns)
    - "replace_parentheses_with_negative": Convert (1,234.56) to -1234.56 (accounting format)
    - "remove_brackets": Remove [ ] and { } brackets
    - "remove_asterisks": Remove * symbols
    - "replace_na_values": Replace N/A, n/a, NA, na, #N/A, #REF!, #VALUE!, etc. with empty string
    - "trim_whitespace": Remove leading/trailing whitespace
    - "remove_spaces": Remove all spaces (for values like "1 234.56")
    - "normalize_negative_signs": Normalize different negative sign unicode characters to standard minus
    
    Args:
        series: pandas Series to clean
        cleaning_steps: List of cleaning operation names
        
    Returns:
        Cleaned Series
    """
    if not cleaning_steps:
        return series
    
    # Convert to string if not already
    if series.dtype != 'object':
        series = series.astype(str)
    else:
        series = series.astype(str)
    
    # Apply cleaning steps in order
    for step in cleaning_steps:
        if step == "remove_dollar_signs":
            series = series.str.replace('$', '', regex=False)
        elif step == "remove_currency_symbols":
            # Remove common currency symbols
            currency_symbols = ['$', '€', '£', '¥', '₹', '₽', '₩', '₪', '₨', '₦', '₨', '₫', '₭', '₮', '₯', '₰', '₱', '₲', '₳', '₴', '₵', '₶', '₷', '₸', '₹', '₺', '₻', '₼', '₽', '₾', '₿']
            for symbol in currency_symbols:
                series = series.str.replace(symbol, '', regex=False)
        elif step == "remove_commas":
            series = series.str.replace(',', '', regex=False)
        elif step == "remove_percent_signs":
            series = series.str.replace('%', '', regex=False)
        elif step == "replace_hash_with_zero":
            series = series.str.replace('#', '0', regex=False)
        elif step == "replace_hash_with_empty":
            series = series.str.replace('#', '', regex=False)
        elif step == "replace_parentheses_with_negative":
            # Convert accounting format (1,234.56) to -1234.56
            # Match pattern: (number) where number can have commas, spaces, etc.
            def replace_parentheses(val):
                if isinstance(val, str) and val.strip().startswith('(') and val.strip().endswith(')'):
                    # Remove parentheses and add negative sign
                    cleaned = val.strip()[1:-1].replace(',', '').replace(' ', '')
                    try:
                        # Try to convert to float to validate, then return as negative string
                        float_val = float(cleaned)
                        return f"-{cleaned}"
                    except (ValueError, TypeError):
                        return val
                return val
            series = series.apply(replace_parentheses)
        elif step == "remove_brackets":
            series = series.str.replace('[', '', regex=False).str.replace(']', '', regex=False)
            series = series.str.replace('{', '', regex=False).str.replace('}', '', regex=False)
        elif step == "remove_asterisks":
            series = series.str.replace('*', '', regex=False)
        elif step == "replace_na_values":
            # Replace common NA/error values with empty string
            na_values = ['N/A', 'n/a', 'NA', 'na', '#N/A', '#REF!', '#VALUE!', '#DIV/0!', '#NAME?', '#NULL!', '#NUM!', 'NULL', 'null', 'None', 'none']
            for na_val in na_values:
                series = series.str.replace(na_val, '', regex=False, case=False)
        elif step == "trim_whitespace":
            series = series.str.strip()
        elif step == "remove_spaces":
            series = series.str.replace(' ', '', regex=False)
        elif step == "normalize_negative_signs":
            # Normalize different unicode minus/negative signs to standard minus
            negative_signs = ['−', '–', '—', '―', '⁻']  # Various unicode minus/negative signs
            for neg_sign in negative_signs:
                series = series.str.replace(neg_sign, '-', regex=False)
        else:
            logger.warning(f"Unknown cleaning step: {step}, skipping")
    
    return series


def _clean_and_convert_numeric(series: pd.Series, target_type: str = "float") -> pd.Series:
    """
    Clean and convert a series containing numeric values (including comma-separated, dollar signs, hash symbols).
    
    Handles:
    - Comma-separated numbers: "1,234.56" -> 1234.56
    - Dollar signs: "$ 1,234.56" or "$ -4,310" -> 1234.56 or -4310
    - Hash symbols (#): "#" -> 0 (for numeric columns)
    - String numbers: "100" -> 100
    - Already numeric values (preserved as-is)
    - Special characters are converted to NaN
    
    Args:
        series: pandas Series to convert
        target_type: "int" or "float" (default: "float")
        
    Returns:
        Converted Series with proper numeric type
    """
    # If already numeric, just convert to target type
    if pd.api.types.is_numeric_dtype(series):
        if target_type.lower() == "int":
            return series.astype("Int64")  # Nullable integer
        else:
            return series.astype("float64")
    
    # Convert to string first to handle all cases
    series_str = series.astype(str)
    
    # Replace hash symbols with 0 (for numeric columns, # typically means "not assigned" or "none")
    series_cleaned = series_str.str.replace('#', '0', regex=False)
    
    # Remove dollar signs and currency symbols
    series_cleaned = series_cleaned.str.replace('$', '', regex=False)
    
    # Remove commas and whitespace
    series_cleaned = series_cleaned.str.replace(',', '', regex=False)
    series_cleaned = series_cleaned.str.strip()
    
    # Replace empty strings and common non-numeric values with NaN
    series_cleaned = series_cleaned.replace(['', 'nan', 'None', 'null', 'NULL', 'NaN'], pd.NA)
    
    # Convert to numeric (handles remaining non-numeric as NaN)
    series_numeric = pd.to_numeric(series_cleaned, errors='coerce')
    
    # Convert to target type
    if target_type.lower() == "int":
        return series_numeric.astype("Int64")  # Nullable integer
    else:
        return series_numeric  # float64


async def _detect_and_normalize_columns_with_llm(
    df: pd.DataFrame,
    table_name: str,
    node_name: str,
    query_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Unified LLM function to detect and normalize both date columns and column types.
    
    This function handles:
    1. Date column detection and normalization to datetime64[ns]
    2. Column type detection (mixed types) and normalization to prevent DuckDB casting errors
    
    Args:
        df: Raw DataFrame (no normalization applied)
        table_name: Name of the table/sheet
        node_name: Node name for logging
        query_id: Optional query ID for LLM tracking
        
    Returns:
        DataFrame with dates and column types normalized
    """
    if df.empty:
        return df
    
    # Get column names and sample data (20 rows) - use raw data for LLM analysis
    column_names = list(df.columns)
    sample_df = df.head(20)
    sample_data = sample_df.to_dict('records')
    
    # Get current dtypes
    current_dtypes = {col: str(df[col].dtype) for col in column_names}
    
    logger.info(
        f"[{node_name}] Calling LLM to detect dates and column types for table '{table_name}' "
        f"({len(column_names)} columns)"
    )
    
    try:
        llm_client = state.get("llm_client") or AzureOpenAIClient()
        model_name = settings.analytics_load_data_model
        
        user_prompt = get_column_normalization_user_prompt(
            column_names=column_names,
            sample_data=sample_data,
            current_dtypes=current_dtypes
        )
        save_llm_call_input(
            node_name=node_name,
            query_id=query_id,
            system_prompt=COLUMN_NORMALIZATION_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            extra={"model": model_name},
        )
        response = await llm_client._call_llm_unified(
            model=model_name,
            system_prompt=COLUMN_NORMALIZATION_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            node_name=node_name,
            query_id=query_id,
            temperature=0.0,
            use_json_mode=True
        )
        
        # Parse LLM response
        try:
            # Extract JSON from response (handle markdown code blocks)
            cleaned_response = response.strip()
            if "```" in cleaned_response:
                code_block_start = cleaned_response.find("```")
                code_block_end = cleaned_response.find("```", code_block_start + 3)
                if code_block_end != -1:
                    code_content = cleaned_response[code_block_start + 3:code_block_end].strip()
                    if code_content.lower().startswith("json"):
                        code_content = code_content[4:].strip()
                    cleaned_response = code_content
            
            detection_result = json.loads(cleaned_response)
            save_llm_call_output(
                node_name=node_name,
                query_id=query_id,
                raw_response=response,
                parsed=detection_result,
            )
        except json.JSONDecodeError as e:
            logger.error(f"[{node_name}] Failed to parse LLM response as JSON: {e}")
            logger.debug(f"[{node_name}] LLM response: {response[:500]}")
            # Fallback: return DataFrame as-is
            return df
        
        if not isinstance(detection_result, dict):
            logger.warning(f"[{node_name}] Invalid LLM response format, skipping normalization")
            return df
        
        # Apply normalization based on LLM suggestions (start with a copy of the input DataFrame)
        df_normalized = df.copy()
        date_normalized_count = 0
        type_normalized_count = 0
        
        # First, handle date columns
        for col_info in detection_result.get("date_columns", []):
            if not isinstance(col_info, dict):
                continue
            
            col_name = col_info.get("column_name")
            is_date = col_info.get("is_date", False)
            normalization_method = col_info.get("normalization_method", "")
            detected_format = col_info.get("detected_format", "")
            confidence = col_info.get("confidence", "low")
            
            if not col_name or not is_date or col_name not in df_normalized.columns:
                continue
            
            if confidence == "low":
                logger.debug(f"[{node_name}] Skipping date column '{col_name}' (low confidence)")
                continue
            
            try:
                logger.info(f"[{node_name}] Normalizing date column '{col_name}' using LLM-suggested method: {normalization_method[:150]}")
                logger.info(f"[{node_name}] Detected format: {detected_format}")
                
                original_series = df_normalized[col_name].copy()
                
                # Try to execute the LLM-suggested normalization method directly
                # The LLM provides pandas code snippets that we need to execute safely
                try:
                    # Replace df[col] or df['col'] with the actual series
                    # Handle common patterns in LLM suggestions
                    normalized_series = None
                    
                    # Pattern: pd.to_datetime(df[col] + '-01', format='%Y-%m-%d')
                    if "+ '-01'" in normalization_method or "+ \"-01\"" in normalization_method:
                        # Extract format if specified
                        format_match = re.search(r'format=["\']([^"\']+)["\']', normalization_method)
                        if format_match:
                            format_str = format_match.group(1)
                            logger.info(f"[{node_name}] Executing: append '-01' with format '{format_str}'")
                            normalized_series = pd.to_datetime(
                                df_normalized[col_name].astype(str) + '-01',
                                format=format_str,
                                errors="coerce"
                            )
                        else:
                            logger.info(f"[{node_name}] Executing: append '-01' with default format")
                            normalized_series = pd.to_datetime(
                                df_normalized[col_name].astype(str) + '-01',
                                format='%Y-%m-%d',
                                errors="coerce"
                            )
                    
                    # Pattern: pd.to_datetime(df[col], unit="D", origin="1899-12-30")
                    elif "unit=" in normalization_method:
                        unit_match = re.search(r'unit=["\']([^"\']+)["\']', normalization_method)
                        origin_match = re.search(r'origin=["\']([^"\']+)["\']', normalization_method)
                        
                        unit = unit_match.group(1) if unit_match else "D"
                        origin = origin_match.group(1) if origin_match else None
                        
                        logger.info(f"[{node_name}] Executing: unit='{unit}', origin='{origin}'")
                        if origin:
                            normalized_series = pd.to_datetime(
                                df_normalized[col_name],
                                unit=unit,
                                origin=origin,
                                errors="coerce"
                            )
                        else:
                            normalized_series = pd.to_datetime(
                                df_normalized[col_name],
                                unit=unit,
                                errors="coerce"
                            )
                    
                    # Pattern: pd.to_datetime(df[col].astype(str), format="%Y%m%d")
                    elif "format=" in normalization_method:
                        format_match = re.search(r'format=["\']([^"\']+)["\']', normalization_method)
                        if format_match:
                            format_str = format_match.group(1)
                            logger.info(f"[{node_name}] Executing: format='{format_str}'")
                            
                            if ".astype(str)" in normalization_method:
                                normalized_series = pd.to_datetime(
                                    df_normalized[col_name].astype(str),
                                    format=format_str,
                                    errors="coerce"
                                )
                            else:
                                normalized_series = pd.to_datetime(
                                    df_normalized[col_name],
                                    format=format_str,
                                    errors="coerce"
                                )
                        else:
                            # Format specified but couldn't extract, try generic
                            logger.warning(f"[{node_name}] Format specified but couldn't extract, using generic pd.to_datetime")
                            normalized_series = pd.to_datetime(
                                df_normalized[col_name],
                                errors="coerce"
                            )
                    
                    # Pattern: Generic pd.to_datetime(df[col])
                    elif "pd.to_datetime" in normalization_method:
                        logger.info(f"[{node_name}] Executing: generic pd.to_datetime")
                        if ".astype(str)" in normalization_method:
                            normalized_series = pd.to_datetime(
                                df_normalized[col_name].astype(str),
                                errors="coerce"
                            )
                        else:
                            normalized_series = pd.to_datetime(
                                df_normalized[col_name],
                                errors="coerce"
                            )
                    
                    # If we couldn't parse the method, try smart multi-format parsing as fallback
                    if normalized_series is None:
                        logger.warning(f"[{node_name}] Could not parse LLM method, using smart multi-format parser")
                        normalized_series = _smart_multi_format_date_parser(original_series, node_name)
                    
                    # Apply the normalization
                    if normalized_series is not None:
                        df_normalized[col_name] = normalized_series
                    else:
                        logger.error(f"[{node_name}] Failed to normalize '{col_name}' - no method worked")
                        continue
                        
                except Exception as e:
                    logger.warning(f"[{node_name}] Failed to execute LLM method, trying smart parser: {e}")
                    # Fallback to smart parser if LLM method fails
                    normalized_series = _smart_multi_format_date_parser(original_series, node_name)
                    if normalized_series is not None:
                        df_normalized[col_name] = normalized_series
                    else:
                        logger.error(f"[{node_name}] Smart parser also failed for '{col_name}'")
                        continue
                
                # Ensure datetime64[ns] dtype
                df_normalized[col_name] = pd.to_datetime(df_normalized[col_name], errors="coerce").astype("datetime64[ns]")
                
                # Verify normalization worked
                non_null_after = df_normalized[col_name].notna().sum()
                if non_null_after > 0:
                    date_normalized_count += 1
                    logger.info(f"[{node_name}] ✓ Normalized date column '{col_name}' to datetime64[ns]")
                else:
                    logger.warning(f"[{node_name}] Normalization of '{col_name}' resulted in no valid dates")
                    
            except Exception as e:
                logger.error(f"[{node_name}] Failed to normalize date column '{col_name}': {e}", exc_info=True)
                continue
        
        # Then, handle column types (skip columns that are now dates)
        for col_info in detection_result.get("column_types", []):
            if not isinstance(col_info, dict):
                continue
            
            col_name = col_info.get("column_name")
            recommended_dtype = col_info.get("recommended_dtype", "")
            conversion_method = col_info.get("conversion_method", "")
            cleaning_steps = col_info.get("cleaning_steps", [])
            confidence = col_info.get("confidence", "low")
            detected_issue = col_info.get("detected_issue", "")
            
            if not col_name or col_name not in df_normalized.columns:
                continue
            
            # Skip date columns (already normalized)
            if pd.api.types.is_datetime64_any_dtype(df_normalized[col_name]):
                continue
            
            if confidence == "low":
                logger.debug(f"[{node_name}] Skipping column '{col_name}' (low confidence)")
                continue
            
            try:
                logger.info(
                    f"[{node_name}] Normalizing column '{col_name}' "
                    f"(issue: {detected_issue[:100] if detected_issue else 'N/A'}) "
                    f"to {recommended_dtype}"
                )
                if cleaning_steps:
                    logger.info(f"[{node_name}]   Cleaning steps: {', '.join(cleaning_steps)}")
                
                original_series = df_normalized[col_name].copy()
                original_dtype = str(original_series.dtype)
                
                # Step 1: Apply cleaning steps first (if any)
                if cleaning_steps:
                    df_normalized[col_name] = _apply_cleaning_steps(df_normalized[col_name], cleaning_steps)
                    logger.debug(f"[{node_name}]   Applied cleaning steps: {', '.join(cleaning_steps)}")
                
                # Step 2: Apply type conversion based on recommended method
                # Most common case: convert to string for mixed types
                if "astype(str)" in conversion_method or recommended_dtype.lower() in ["string", "str", "object"]:
                    df_normalized[col_name] = df_normalized[col_name].astype(str)
                    logger.debug(f"[{node_name}]   Converted '{col_name}' from {original_dtype} to string")
                
                # Convert to numeric (with coercion for invalid values)
                elif "to_numeric" in conversion_method or recommended_dtype.lower() in ["int64", "float64", "numeric"]:
                    # Use helper function to handle numeric conversion (already handles commas, dollar signs, hash)
                    if "int" in recommended_dtype.lower():
                        df_normalized[col_name] = _clean_and_convert_numeric(df_normalized[col_name], target_type="int")
                    else:
                        df_normalized[col_name] = _clean_and_convert_numeric(df_normalized[col_name], target_type="float")
                    logger.debug(f"[{node_name}]   Converted '{col_name}' from {original_dtype} to {recommended_dtype}")
                
                # Generic conversion attempt
                else:
                    logger.warning(
                        f"[{node_name}] Unknown conversion method for '{col_name}', "
                        f"trying generic conversion to {recommended_dtype}"
                    )
                    try:
                        if recommended_dtype.lower() in ["string", "str"]:
                            df_normalized[col_name] = df_normalized[col_name].astype(str)
                        elif "int" in recommended_dtype.lower():
                            df_normalized[col_name] = _clean_and_convert_numeric(df_normalized[col_name], target_type="int")
                        elif "float" in recommended_dtype.lower():
                            df_normalized[col_name] = _clean_and_convert_numeric(df_normalized[col_name], target_type="float")
                        else:
                            logger.warning(f"[{node_name}]   Could not determine conversion for '{col_name}', leaving as-is")
                            continue
                    except Exception as e:
                        logger.warning(f"[{node_name}]   Failed to convert '{col_name}': {e}")
                        continue
                
                # Verify conversion worked
                new_dtype = str(df_normalized[col_name].dtype)
                if new_dtype != original_dtype:
                    type_normalized_count += 1
                    logger.info(
                        f"[{node_name}] ✓ Normalized '{col_name}': {original_dtype} → {new_dtype}"
                    )
                else:
                    logger.debug(f"[{node_name}] Column '{col_name}' dtype unchanged ({original_dtype})")
                    
            except Exception as e:
                logger.error(f"[{node_name}] Failed to normalize column '{col_name}': {e}", exc_info=True)
                continue
        
        # Additional pass: Auto-convert object columns that are actually numeric
        # (handles cases where LLM might have missed them or they weren't in the sample)
        logger.debug(f"[{node_name}] Checking for additional object columns that should be numeric...")
        auto_converted_count = 0
        
        for col in df_normalized.columns:
            # Skip date columns and already processed columns
            if pd.api.types.is_datetime64_any_dtype(df_normalized[col]):
                continue
            
            # Only process object-type columns
            if df_normalized[col].dtype != 'object':
                continue
            
            # Skip if LLM already processed this column
            llm_processed = False
            for col_info in detection_result.get("column_types", []):
                if isinstance(col_info, dict) and col_info.get("column_name") == col:
                    llm_processed = True
                    break
            if llm_processed:
                continue
            
            # Check if column contains mostly numeric values (including comma-separated)
            non_null_values = df_normalized[col].dropna()
            if len(non_null_values) == 0:
                continue
            
            numeric_count = 0
            total_count = len(non_null_values)
            
            for val in non_null_values:
                if isinstance(val, (int, float)):
                    numeric_count += 1
                elif isinstance(val, str):
                    # Try to detect if value is numeric (handle common formats)
                    val_cleaned = val.replace('$', '').replace(',', '').replace('#', '0').strip()
                    if val_cleaned == '' or val_cleaned == '0':
                        # Hash symbol or empty, count as numeric (will be converted to 0)
                        numeric_count += 1
                    else:
                        try:
                            float(val_cleaned)
                            numeric_count += 1
                        except (ValueError, TypeError):
                            pass
            
            # If >80% of values are numeric, convert to numeric type
            if numeric_count / total_count > 0.8:
                try:
                    original_dtype = str(df_normalized[col].dtype)
                    # Check if values have decimals (after cleaning)
                    has_decimals = False
                    for val in non_null_values[:100]:  # Check first 100 values
                        if isinstance(val, str):
                            # Apply common cleaning to check for decimals
                            val_cleaned = val.replace('$', '').replace(',', '').replace('#', '0').strip()
                            if '.' in val_cleaned:
                                try:
                                    float(val_cleaned)
                                    has_decimals = True
                                    break
                                except (ValueError, TypeError):
                                    pass
                    
                    if has_decimals:
                        df_normalized[col] = _clean_and_convert_numeric(df_normalized[col], target_type="float")
                        logger.info(
                            f"[{node_name}] ✓ Auto-converted object column '{col}' to float64 "
                            f"({numeric_count}/{total_count} values are numeric, including comma-separated)"
                        )
                    else:
                        df_normalized[col] = _clean_and_convert_numeric(df_normalized[col], target_type="int")
                        logger.info(
                            f"[{node_name}] ✓ Auto-converted object column '{col}' to int64 "
                            f"({numeric_count}/{total_count} values are numeric)"
                        )
                    auto_converted_count += 1
                except Exception as e:
                    logger.debug(f"[{node_name}] Failed to auto-convert '{col}': {e}")
                    continue
        
        if date_normalized_count > 0 or type_normalized_count > 0 or auto_converted_count > 0:
            logger.info(
                f"[{node_name}] Normalized {date_normalized_count} date column(s), "
                f"{type_normalized_count} type column(s), and "
                f"{auto_converted_count} auto-converted column(s) for table '{table_name}'"
            )
        else:
            logger.info(
                f"[{node_name}] No columns detected or normalized for table '{table_name}'"
            )
        
        return df_normalized
        
    except Exception as e:
        logger.error(f"[{node_name}] LLM column normalization failed: {e}", exc_info=True)
        # Return DataFrame as-is if LLM fails
        return df


async def load_data_node(state: AnalyticsState) -> Dict[str, Any]:
    """
    Load and normalize Excel/CSV data exactly once per query using LLM for unified column analysis.
    
    This node:
    1. Loads raw Excel/CSV files (no normalization)
    2. Calls LLM to detect date columns and column type issues in a single unified call
    3. Applies LLM-suggested normalization for dates and column types
    4. Stores normalized DataFrames in state["dataframes"]
    
    The unified LLM call handles:
    - Date column detection and normalization to datetime64[ns]
    - Column type detection (mixed types like '*' with numbers) and normalization to prevent DuckDB casting errors
    
    Args:
        state: Current analytics state containing:
            - selected_tables: List of table names
            - data_source_config: Data source configuration
            
    Returns:
        Updated state dictionary with:
            - dataframes: Dict[str, pd.DataFrame] - Normalized DataFrames per table
            - status: "data_loaded" on success, "error" on failure
    """
    start_time = datetime.now()
    node_name = "load_data"
    
    # Record timing
    from ..node_timing_registry import get_node_timing_registry
    registry = get_node_timing_registry()
    if registry:
        registry.record_node_start(node_name, start_time)
    
    logger.info(f"[{node_name}] Starting single-pass data loading & LLM-based normalization")
    
    # Get WebSocket manager for progress updates
    ws_manager = state.get("ws_manager")
    
    # Skip if errors present
    if state.get("errors"):
        logger.warning(f"[{node_name}] Errors present, skipping")
        return {}

    # Skip when data source is SAP (no file to load; schema comes from SAP APIs)
    data_source_config = state.get("data_source_config") or {}
    data_source_type = (data_source_config.get("type") or "").lower()
    if data_source_type in ("sap", "sap_datasphere"):
        logger.info(f"[{node_name}] SAP data source - skipping (no file load)")
        return {"dataframes": {}, "status": "skipped"}

    logger.info(f"[{node_name}] ========== Starting Data Loading & Normalization ==========")
    
    # Send initial progress message
    if ws_manager:
        try:
            await ws_manager.send_progress(
                node_name=node_name,
                message="Loading and cleaning data files",
                status="processing",
                details="Starting data loading process..."
            )
        except Exception as e:
            logger.warning(f"[{node_name}] Failed to send initial progress: {e}")
    
    # Check if dataframes already exist (reuse existing)
    existing_dataframes = state.get("dataframes", {})
    existing_date_ranges = state.get("available_date_ranges", {})
    if existing_dataframes:
        logger.info(
            f"[{node_name}] ✅ DataFrames already loaded, reusing {len(existing_dataframes)} table(s): "
            f"{list(existing_dataframes.keys())}"
        )
        total_rows = sum(len(df) for df in existing_dataframes.values())
        logger.info(f"[{node_name}] 📊 Total cached rows: {total_rows:,}")
        return {
            "dataframes": existing_dataframes,
            "available_date_ranges": existing_date_ranges,
            "status": "data_loaded",
        }
    
    # Get data source configuration
    data_source_config = state.get("data_source_config")
    if not data_source_config:
        logger.warning(f"[{node_name}] ⚠️ No data source configuration - skipping (not Excel/CSV)")
        return {
            "dataframes": {},
            "status": "skipped",
        }
    
    data_source_type = data_source_config.get("type", "").lower()
    logger.info(f"[{node_name}] 📊 Data Source Type: {data_source_type.upper()}")
    
    # Only process Excel/CSV sources
    if data_source_type not in ("excel", "csv"):
        logger.info(f"[{node_name}] ℹ️  Data source type '{data_source_type}' is not Excel/CSV - skipping")
        return {
            "dataframes": {},
            "status": "skipped",
        }
    
    selected_tables = state.get("selected_tables", [])
    if not selected_tables:
        logger.warning(f"[{node_name}] ⚠️ No tables selected - skipping")
        return {
            "dataframes": {},
            "status": "skipped",
        }
    
    logger.info(f"[{node_name}] 📋 Selected tables: {', '.join(selected_tables)}")
    
    file_path = data_source_config.get("file_path")
    if not file_path or not Path(file_path).exists():
        logger.error(f"[{node_name}] ❌ File not found: {file_path}")
        return {
            "errors": state.get("errors", []) + [f"File not found: {file_path}"],
            "status": "error",
        }
    
    logger.info(f"[{node_name}] 📁 Loading from file: {file_path}")
    
    dataframes = {}
    available_date_ranges = {}
    query_id = state.get("query_id")
    
    # Get cleaned data storage
    cleaned_storage = get_cleaned_data_cache()
    
    # Check if cleaned file exists - if yes, use it directly
    cleaned_file_path = cleaned_storage.get_cleaned_file_path(file_path)
    
    # Track progress metrics
    files_processed = 0
    columns_cleaned = 0
    sheets_processed = 0
    
    try:
        if data_source_type == "excel":
            logger.info(f"[{node_name}] 📊 Processing Excel file...")
            
            # Send progress about file being processed
            if ws_manager:
                try:
                    await ws_manager.send_progress(
                        node_name=node_name,
                        message="Processing Excel file",
                        status="processing",
                        details=f"Analyzing file: {Path(file_path).name}"
                    )
                except Exception as e:
                    logger.warning(f"[{node_name}] Failed to send file progress: {e}")
            
            # Check if cleaned file exists - use it if available
            if cleaned_file_path:
                logger.info(f"[{node_name}] ✅ Using cleaned Excel file: {Path(cleaned_file_path).name}")
                file_to_use = cleaned_file_path
                is_cleaned_file = True
                
                if ws_manager:
                    try:
                        await ws_manager.send_progress(
                            node_name=node_name,
                            message="Using cleaned file",
                            status="processing",
                            details=f"Found cleaned file: {Path(cleaned_file_path).name} (no cleaning needed)"
                        )
                    except Exception as e:
                        logger.warning(f"[{node_name}] Failed to send cleaned file progress: {e}")
            else:
                logger.info(f"[{node_name}] 📥 No cleaned file found, will clean original file: {Path(file_path).name}")
                file_to_use = file_path
                is_cleaned_file = False
                files_processed = 1  # Will process this file
            
            # Get appropriate engine using data_source_gateway function
            engine = get_excel_file_engine(file_to_use)
            logger.debug(f"[{node_name}]   Using Excel engine: {engine}")
            try:
                xl_file = pd.ExcelFile(file_to_use, engine=engine)
            except Exception as e:
                # Try alternative engine
                alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                logger.warning(f"[{node_name}]   ⚠️ Failed with {engine} engine, trying {alt_engine}: {str(e)}")
                try:
                    xl_file = pd.ExcelFile(file_to_use, engine=alt_engine)
                    engine = alt_engine
                    logger.info(f"[{node_name}]   ✅ Successfully opened with {alt_engine} engine")
                except Exception as e2:
                    raise DatabaseException(
                        f"Failed to open Excel file with both engines. "
                        f"openpyxl error: {str(e)}, {alt_engine} error: {str(e2)}"
                    ) from e2
            
            sheet_names = xl_file.sheet_names
            if not sheet_names:
                raise DatabaseException("Excel file contains no sheets")
            
            logger.info(f"[{node_name}]   📋 Found {len(sheet_names)} sheet(s): {', '.join(sheet_names)}")
            
            # Load all selected sheets (or all sheets if selected_tables is empty)
            sheets_to_load = selected_tables if selected_tables else sheet_names
            logger.info(f"[{node_name}]   🎯 Loading {len(sheets_to_load)} selected sheet(s): {', '.join(sheets_to_load)}")
            
            # If using cleaned file, just load it without cleaning
            if is_cleaned_file:
                logger.info(f"[{node_name}]   ✅ Loading from cleaned file (no cleaning needed)")
                total_columns = 0
                for sheet_name in sheets_to_load:
                    if sheet_name not in sheet_names:
                        logger.warning(f"[{node_name}] ⚠️ Sheet '{sheet_name}' not found in Excel file, skipping")
                        continue
                    
                    try:
                        logger.info(f"[{node_name}] 📥 Loading cleaned sheet: '{sheet_name}'...")
                        df_normalized = read_excel_with_engine(file_to_use, sheet_name=sheet_name, engine=engine)
                        
                        if df_normalized.empty:
                            logger.warning(f"[{node_name}] ⚠️ Sheet '{sheet_name}' is empty, skipping")
                            continue
                        
                        dataframes[sheet_name] = df_normalized
                        sheets_processed += 1
                        total_columns += len(df_normalized.columns)
                        logger.info(
                            f"[{node_name}]   ✅ Loaded cleaned sheet '{sheet_name}': "
                            f"{len(df_normalized):,} rows, {len(df_normalized.columns)} columns"
                        )
                        
                        # Send progress update for each sheet loaded
                        if ws_manager:
                            try:
                                await ws_manager.send_progress(
                                    node_name=node_name,
                                    message=f"Loaded sheet: {sheet_name}",
                                    status="processing",
                                    details=f"Sheet {sheets_processed}/{len(sheets_to_load)}: {len(df_normalized):,} rows, {len(df_normalized.columns)} columns"
                                )
                            except Exception as e:
                                logger.warning(f"[{node_name}] Failed to send sheet progress: {e}")
                        
                        # Extract date ranges
                        date_cols = [c for c in df_normalized.columns if pd.api.types.is_datetime64_any_dtype(df_normalized[c])]
                        if date_cols:
                            min_dates = []
                            max_dates = []
                            for col in date_cols:
                                col_min = df_normalized[col].min()
                                col_max = df_normalized[col].max()
                                if pd.notna(col_min) and pd.notna(col_max):
                                    min_dates.append(col_min)
                                    max_dates.append(col_max)
                            
                            if min_dates and max_dates:
                                overall_min = min(min_dates)
                                overall_max = max(max_dates)
                                available_date_ranges[sheet_name] = {
                                    "min_date": overall_min.isoformat() if hasattr(overall_min, 'isoformat') else str(overall_min),
                                    "max_date": overall_max.isoformat() if hasattr(overall_max, 'isoformat') else str(overall_max),
                                    "date_columns": date_cols
                                }
                    except Exception as e:
                        logger.error(f"[{node_name}] Failed to load sheet '{sheet_name}': {e}")
                        continue
                
                # Send final progress for cleaned file
                if ws_manager and sheets_processed > 0:
                    try:
                        await ws_manager.send_progress(
                            node_name=node_name,
                            message="Loaded cleaned file",
                            status="processing",
                            details=f"Processed {sheets_processed} sheet(s) with {total_columns} total columns (from cleaned file)"
                        )
                    except Exception as e:
                        logger.warning(f"[{node_name}] Failed to send final progress: {e}")
            else:
                # Need to clean the original file
                total_columns_before = 0
                total_columns_after = 0
                for sheet_name in sheets_to_load:
                    if sheet_name not in sheet_names:
                        logger.warning(f"[{node_name}] ⚠️ Sheet '{sheet_name}' not found in Excel file, skipping")
                        continue
                    
                    try:
                        # Load and clean data (no cleaned file exists, so we need to process original)
                        logger.info(f"[{node_name}] 📥 Loading and cleaning sheet: '{sheet_name}'...")
                        
                        # Load raw data (no normalization) using data_source_gateway function
                        df_raw = read_excel_with_engine(file_path, sheet_name=sheet_name, engine=engine)
                        
                        if df_raw.empty:
                            logger.warning(f"[{node_name}] ⚠️ Sheet '{sheet_name}' is empty, skipping")
                            continue
                        
                        logger.info(
                            f"[{node_name}]   ✅ Loaded raw data: {len(df_raw):,} rows, {len(df_raw.columns)} columns"
                        )
                        logger.debug(f"[{node_name}]      Columns: {', '.join(df_raw.columns[:10])}{'...' if len(df_raw.columns) > 10 else ''}")
                        
                        # Use LLM to detect and normalize dates and column types (unified)
                        logger.info(f"[{node_name}]   🔍 Detecting and normalizing dates and column types using LLM...")
                        df_normalized = await _detect_and_normalize_columns_with_llm(
                            df_raw,
                            table_name=sheet_name,
                            node_name=node_name,
                            query_id=query_id
                        )
                        
                        logger.info(
                            f"[{node_name}]   ✅ Column normalization complete for '{sheet_name}'"
                        )
                        
                        # Store normalized DataFrame in memory
                        dataframes[sheet_name] = df_normalized
                        
                        # Extract date ranges for this sheet
                        date_cols = [c for c in df_normalized.columns if pd.api.types.is_datetime64_any_dtype(df_normalized[c])]
                        if date_cols:
                            min_dates = []
                            max_dates = []
                            for col in date_cols:
                                col_min = df_normalized[col].min()
                                col_max = df_normalized[col].max()
                                if pd.notna(col_min) and pd.notna(col_max):
                                    min_dates.append(col_min)
                                    max_dates.append(col_max)
                            
                            if min_dates and max_dates:
                                overall_min = min(min_dates)
                                overall_max = max(max_dates)
                                available_date_ranges[sheet_name] = {
                                    "min_date": overall_min.isoformat() if hasattr(overall_min, 'isoformat') else str(overall_min),
                                    "max_date": overall_max.isoformat() if hasattr(overall_max, 'isoformat') else str(overall_max),
                                    "date_columns": date_cols
                                }
                    
                    except Exception as e:
                        logger.error(f"[{node_name}] Failed to load sheet '{sheet_name}': {e}")
                        continue
                
                # After cleaning all sheets, save to cleaned Excel file
                if dataframes:
                    cleaned_file_path = cleaned_storage.save_cleaned_excel(
                        original_file_path=file_path,
                        dataframes=dataframes,
                        engine=engine
                    )
                    if cleaned_file_path:
                        logger.info(
                            f"[{node_name}]   💾 Saved cleaned Excel file: {Path(cleaned_file_path).name} "
                            f"({len(dataframes)} sheet(s))"
                        )
                        
                        # Send progress about saving cleaned file
                        if ws_manager:
                            try:
                                await ws_manager.send_progress(
                                    node_name=node_name,
                                    message="Saved cleaned file",
                                    status="processing",
                                    details=f"Saved cleaned Excel file with {len(dataframes)} sheet(s) and {columns_cleaned} columns"
                                )
                            except Exception as e:
                                logger.warning(f"[{node_name}] Failed to send save progress: {e}")
            
            if not dataframes:
                logger.error(f"[{node_name}] ❌ Failed to load any sheets from Excel file - no data available")
                return {
                    "errors": state.get("errors", []) + ["No data available to process - failed to load any sheets"],
                    "status": "error",
                    "no_data_available": True,
                    "dataframes": {},
                    "available_date_ranges": {},
                }
            
            total_rows = sum(len(df) for df in dataframes.values())
            total_cols = sum(len(df.columns) for df in dataframes.values())
            logger.info(
                f"[{node_name}] ========== Excel Loading Complete =========="
            )
            logger.info(
                f"[{node_name}] 📊 Summary: {len(dataframes)} sheet(s) loaded, "
                f"{total_rows:,} total rows, {total_cols} total columns"
            )
            logger.info(
                f"[{node_name}] 📋 Sheets: {', '.join(dataframes.keys())}"
            )
            logger.info(
                f"[{node_name}] 💾 All DataFrames cached in state['dataframes'] for reuse"
            )
            
        elif data_source_type == "csv":
            logger.info(f"[{node_name}] 📊 Processing CSV file...")
            
            # Send progress about CSV file
            if ws_manager:
                try:
                    await ws_manager.send_progress(
                        node_name=node_name,
                        message="Processing CSV file",
                        status="processing",
                        details=f"Analyzing file: {Path(file_path).name}"
                    )
                except Exception as e:
                    logger.warning(f"[{node_name}] Failed to send CSV progress: {e}")
            
            try:
                # Determine table name from file name or selected_tables
                if selected_tables:
                    table_name = selected_tables[0]
                else:
                    table_name = Path(file_path).stem
                
                logger.info(f"[{node_name}]   📋 Table name: '{table_name}'")
                
                # Check if cleaned file exists - use it if available
                if cleaned_file_path:
                    logger.info(f"[{node_name}]   ✅ Using cleaned CSV file: {Path(cleaned_file_path).name} (no cleaning needed)")
                    
                    if ws_manager:
                        try:
                            await ws_manager.send_progress(
                                node_name=node_name,
                                message="Using cleaned CSV file",
                                status="processing",
                                details=f"Found cleaned file: {Path(cleaned_file_path).name} (no cleaning needed)"
                            )
                        except Exception as e:
                            logger.warning(f"[{node_name}] Failed to send cleaned CSV progress: {e}")
                    
                    df_normalized = read_csv_with_encoding(cleaned_file_path)
                    
                    if df_normalized.empty:
                        logger.error(f"[{node_name}] ❌ Cleaned CSV file is empty")
                        return {
                            "errors": state.get("errors", []) + ["Cleaned CSV file is empty"],
                            "status": "error",
                            "no_data_available": True,
                            "dataframes": {},
                            "available_date_ranges": {},
                        }
                    
                    dataframes[table_name] = df_normalized
                    files_processed = 1
                    sheets_processed = 1
                    columns_cleaned = len(df_normalized.columns)
                    logger.info(
                        f"[{node_name}]   ✅ Loaded cleaned CSV: {len(df_normalized):,} rows, {len(df_normalized.columns)} columns"
                    )
                    
                    if ws_manager:
                        try:
                            await ws_manager.send_progress(
                                node_name=node_name,
                                message="Loaded cleaned CSV",
                                status="processing",
                                details=f"Loaded {len(df_normalized):,} rows with {len(df_normalized.columns)} columns (from cleaned file)"
                            )
                        except Exception as e:
                            logger.warning(f"[{node_name}] Failed to send loaded CSV progress: {e}")
                    
                    # Extract date ranges
                    date_cols = [c for c in df_normalized.columns if pd.api.types.is_datetime64_any_dtype(df_normalized[c])]
                    if date_cols:
                        min_dates = []
                        max_dates = []
                        for col in date_cols:
                            col_min = df_normalized[col].min()
                            col_max = df_normalized[col].max()
                            if pd.notna(col_min) and pd.notna(col_max):
                                min_dates.append(col_min)
                                max_dates.append(col_max)
                        
                        if min_dates and max_dates:
                            overall_min = min(min_dates)
                            overall_max = max(max_dates)
                            available_date_ranges[table_name] = {
                                "min_date": overall_min.isoformat() if hasattr(overall_min, 'isoformat') else str(overall_min),
                                "max_date": overall_max.isoformat() if hasattr(overall_max, 'isoformat') else str(overall_max),
                                "date_columns": date_cols
                            }
                else:
                    # No cleaned file - need to clean the original
                    logger.info(f"[{node_name}]   📥 No cleaned file found, cleaning original CSV: {Path(file_path).name}")
                    files_processed = 1
                    
                    # Send progress about cleaning
                    if ws_manager:
                        try:
                            await ws_manager.send_progress(
                                node_name=node_name,
                                message="Cleaning CSV file",
                                status="processing",
                                details=f"Cleaning {Path(file_path).name}..."
                            )
                        except Exception as e:
                            logger.warning(f"[{node_name}] Failed to send cleaning progress: {e}")
                    
                    # Load raw data (no normalization) using data_source_gateway function
                    df_raw = read_csv_with_encoding(file_path)
                    
                    if df_raw.empty:
                        logger.error(f"[{node_name}] ❌ CSV file is empty - no data available to process")
                        return {
                            "errors": state.get("errors", []) + ["No data available to process - CSV file is empty"],
                            "status": "error",
                            "no_data_available": True,
                            "dataframes": {},
                            "available_date_ranges": {},
                        }
                    
                    columns_before = len(df_raw.columns)
                    logger.info(
                        f"[{node_name}]   ✅ Loaded raw data: {len(df_raw):,} rows, {len(df_raw.columns)} columns"
                    )
                    logger.debug(f"[{node_name}]      Columns: {', '.join(df_raw.columns[:10])}{'...' if len(df_raw.columns) > 10 else ''}")
                    
                    # Send progress about cleaning columns
                    if ws_manager:
                        try:
                            await ws_manager.send_progress(
                                node_name=node_name,
                                message="Cleaning columns",
                                status="processing",
                                details=f"Cleaning {columns_before} columns using LLM..."
                            )
                        except Exception as e:
                            logger.warning(f"[{node_name}] Failed to send column cleaning progress: {e}")
                    
                    # Use LLM to detect and normalize dates and column types (unified)
                    logger.info(f"[{node_name}]   🔍 Detecting and normalizing dates and column types using LLM...")
                    df_normalized = await _detect_and_normalize_columns_with_llm(
                        df_raw,
                        table_name=table_name,
                        node_name=node_name,
                        query_id=query_id
                    )
                    
                    columns_cleaned = len(df_normalized.columns)
                    sheets_processed = 1
                    
                    logger.info(
                        f"[{node_name}]   ✅ Column normalization complete for '{table_name}'"
                    )
                    
                    # Send progress after cleaning
                    if ws_manager:
                        try:
                            await ws_manager.send_progress(
                                node_name=node_name,
                                message="Columns cleaned",
                                status="processing",
                                details=f"Cleaned {columns_cleaned} columns successfully"
                            )
                        except Exception as e:
                            logger.warning(f"[{node_name}] Failed to send cleaned progress: {e}")
                    
                    # Store normalized DataFrame in memory
                    dataframes[table_name] = df_normalized
                    
                    # Save to cleaned CSV file for future queries
                    cleaned_file_path = cleaned_storage.save_cleaned_csv(
                        original_file_path=file_path,
                        df=df_normalized
                    )
                    if cleaned_file_path:
                        logger.info(
                            f"[{node_name}]   💾 Saved cleaned CSV file: {Path(cleaned_file_path).name}"
                        )
                        
                        if ws_manager:
                            try:
                                await ws_manager.send_progress(
                                    node_name=node_name,
                                    message="Saved cleaned CSV file",
                                    status="processing",
                                    details=f"Saved cleaned CSV file with {columns_cleaned} columns"
                                )
                            except Exception as e:
                                logger.warning(f"[{node_name}] Failed to send save progress: {e}")
                
                # Extract date ranges from normalized DataFrame
                date_cols = [c for c in df_normalized.columns if pd.api.types.is_datetime64_any_dtype(df_normalized[c])]
                if date_cols:
                    min_dates = []
                    max_dates = []
                    for col in date_cols:
                        col_min = df_normalized[col].min()
                        col_max = df_normalized[col].max()
                        if pd.notna(col_min) and pd.notna(col_max):
                            min_dates.append(col_min)
                            max_dates.append(col_max)
                    
                    if min_dates and max_dates:
                        overall_min = min(min_dates)
                        overall_max = max(max_dates)
                        available_date_ranges[table_name] = {
                            "min_date": overall_min.isoformat() if hasattr(overall_min, 'isoformat') else str(overall_min),
                            "max_date": overall_max.isoformat() if hasattr(overall_max, 'isoformat') else str(overall_max),
                            "date_columns": date_cols
                        }
                        logger.info(
                            f"[{node_name}] ✓ Loaded CSV '{table_name}': "
                            f"{len(df_normalized)} rows, {len(df_normalized.columns)} columns, "
                            f"{len(date_cols)} date columns normalized. "
                            f"Date range: {overall_min.date()} to {overall_max.date()}"
                        )
                    else:
                        logger.info(
                            f"[{node_name}] ✓ Loaded CSV '{table_name}': "
                            f"{len(df_normalized)} rows, {len(df_normalized.columns)} columns, "
                            f"{len(date_cols)} date columns normalized (no valid date range)"
                        )
                else:
                    logger.info(
                        f"[{node_name}] ✓ Loaded CSV '{table_name}': "
                        f"{len(df_normalized)} rows, {len(df_normalized.columns)} columns, "
                        f"no date columns found"
                    )
                
            except Exception as e:
                raise DatabaseException(f"Failed to load CSV file: {str(e)}") from e
            
            if dataframes:
                total_rows = sum(len(df) for df in dataframes.values())
                total_cols = sum(len(df.columns) for df in dataframes.values())
                logger.info(
                    f"[{node_name}] ========== CSV Loading Complete =========="
                )
                logger.info(
                    f"[{node_name}] 📊 Summary: {len(dataframes)} table(s) loaded, "
                    f"{total_rows:,} total rows, {total_cols} total columns"
                )
                logger.info(
                    f"[{node_name}] 📋 Tables: {', '.join(dataframes.keys())}"
                )
                logger.info(
                    f"[{node_name}] 💾 All DataFrames cached in state['dataframes'] for reuse"
                )
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"[{node_name}] ========== Data Loading & Normalization Complete =========="
        )
        logger.info(
            f"[{node_name}] ⏱️  Duration: {duration:.2f}s"
        )
        logger.info(
            f"[{node_name}] 📊 Final Summary: {len(dataframes)} table(s) loaded and normalized"
        )
        if dataframes:
            total_rows = sum(len(df) for df in dataframes.values())
            logger.info(
                f"[{node_name}] 📈 Total rows cached: {total_rows:,}"
            )
        if available_date_ranges:
            logger.info(
                f"[{node_name}] 📅 Date ranges extracted for {len(available_date_ranges)} table(s)"
            )
        
        # Send final completion message with summary
        if ws_manager:
            try:
                total_rows = sum(len(df) for df in dataframes.values()) if dataframes else 0
                total_cols = sum(len(df.columns) for df in dataframes.values()) if dataframes else 0
                
                # Build table/sheet details
                table_details = []
                for table_name, df in dataframes.items():
                    date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
                    table_details.append({
                        "table_name": table_name,
                        "rows": len(df),
                        "columns": len(df.columns),
                        "date_columns": len(date_cols),
                        "status": "success"
                    })
                
                # Build summary details text
                summary_details = f"Processed {files_processed} file(s), {sheets_processed} sheet(s), cleaned {columns_cleaned} columns"
                if total_rows > 0:
                    summary_details += f", {total_rows:,} total rows"
                
                # Prepare structured summary data for WebSocket
                summary_data = {
                    "load_summary": {
                        "files_processed": files_processed,
                        "sheets_processed": sheets_processed,
                        "columns_cleaned": columns_cleaned,
                        "total_tables": len(dataframes) if dataframes else 0,
                        "total_rows": total_rows,
                        "total_columns": total_cols,
                        "duration_seconds": round(duration, 2),
                        "table_details": table_details,
                        "date_ranges_extracted": len(available_date_ranges) if available_date_ranges else 0
                    }
                }
                
                await ws_manager.send_progress(
                    node_name=node_name,
                    message="Data loading complete",
                    status="complete",
                    details=summary_details,
                    data=summary_data
                )
                logger.info(f"[{node_name}] ✅ Sent load summary to frontend with structured data")
            except Exception as e:
                logger.warning(f"[{node_name}] ⚠️ Failed to send completion message: {e}")
        
        return {
            "dataframes": dataframes,
            "available_date_ranges": available_date_ranges if available_date_ranges else {},
            "status": "data_loaded",
        }
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[{node_name}] Failed after {duration:.2f}s: {e}", exc_info=True)
        return {
            "errors": state.get("errors", []) + [f"Data loading failed: {str(e)}"],
            "status": "error",
            "no_data_available": True,
        }
