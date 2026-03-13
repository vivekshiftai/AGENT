"""SAP relational view fetch: count-first + parallel $top/$skip chunking. Uses $count then fetches in chunks."""
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from config.settings import settings
from infrastructure.langgraph.utils.sap_fetch_helpers import (
    clean_odata_select,
    extract_date_columns_from_schema,
)

logger = logging.getLogger(__name__)

MAX_ROWS_PER_PAGE = settings.sap_rows_per_page
MAX_CHUNK_CALLS_PER_VIEW = getattr(settings, "sap_max_chunk_calls", 159)


async def _get_row_count(
    datasphere_service: Any,
    user_id: str,
    view_name: str,
    filter_expr: Optional[str],
    orderby: Optional[str],
    data_url: Optional[str],
    space_id: Optional[str],
    token: str,
    select: Optional[str] = None,
) -> Optional[int]:
    """Get total row count for a view using $count call."""
    select = clean_odata_select(select)
    try:
        logger.info("[SAP Fetch] 📊 Step 1: Getting count for '%s'...", view_name)
        if filter_expr:
            logger.info("[SAP Fetch]    Filter: %s%s", filter_expr[:100], "..." if len(filter_expr) > 100 else "")
        if orderby:
            logger.info("[SAP Fetch]    OrderBy: %s", orderby)
        if select:
            logger.info("[SAP Fetch]    Select: %s%s", select[:100], "..." if len(select) > 100 else "")

        result = await datasphere_service.execute_odata_query(
            user_id=user_id,
            view_name=view_name,
            select=select,
            filter=filter_expr,
            top=0,
            count=True,
            orderby=orderby,
            data_url=data_url,
            space_id=space_id,
            token=token,
        )
        count = result.count
        if count is not None:
            logger.info("[SAP Fetch] ✅ Step 1 Complete: '%s' has %s total rows", view_name, f"{count:,}")
        else:
            logger.warning("[SAP Fetch] ⚠️ Step 1 Failed: '%s' - Count call returned None", view_name)
        return count
    except Exception as e:
        logger.error("[SAP Fetch] ❌ Step 1 Failed: Cannot get count for '%s': %s", view_name, e)
        return None


async def _fetch_data_chunk(
    datasphere_service: Any,
    user_id: str,
    view_name: str,
    filter_expr: Optional[str],
    orderby: Optional[str],
    data_url: Optional[str],
    space_id: Optional[str],
    token: str,
    top: int,
    skip: int,
    chunk_num: int,
    total_chunks: int,
    total_count: Optional[int] = None,
    select: Optional[str] = None,
) -> Optional[Tuple]:
    """Fetch a single chunk of data with $orderby. Returns (lazy_frame, rows_returned, rows_expected, api_url) or None."""
    select = clean_odata_select(select)
    try:
        orderby_info = f", orderby={orderby}" if orderby else " (⚠️ NO ORDERBY)"
        logger.info("[SAP Fetch] 📥 Step 3.%s: Fetching chunk %s/%s for '%s'", chunk_num, chunk_num, total_chunks, view_name)
        logger.info("[SAP Fetch]    Parameters: skip=%s, top=%s%s", f"{skip:,}", f"{top:,}", orderby_info)

        if not orderby:
            logger.warning("[SAP Fetch]    ⚠️ WARNING: No $orderby specified - data may not be consistently ordered!")

        result = await datasphere_service.execute_odata_query(
            user_id=user_id,
            view_name=view_name,
            select=select,
            filter=filter_expr,
            top=top,
            skip=skip,
            orderby=orderby,
            data_url=data_url,
            space_id=space_id,
            token=token,
            total_count=total_count,
        )
        rows_returned = len(result.data) if result.data else 0
        rows_expected = top
        api_url = result.api_url

        if result.data:
            try:
                try:
                    chunk_lf = pl.LazyFrame(result.data, infer_schema_length=None)
                    chunk_lf.collect_schema()
                except Exception as schema_error:
                    logger.debug("[SAP Fetch] Schema inference issue, using DataFrame approach: %s", schema_error)
                    chunk_df = pl.DataFrame(result.data, infer_schema_length=None)
                    chunk_lf = chunk_df.lazy()

                if rows_returned < rows_expected:
                    missing_rows = rows_expected - rows_returned
                    logger.warning(
                        "[SAP Fetch] ⚠️ Step 3.%s Partial: Chunk %s/%s for '%s': Requested %s rows but only got %s (missing %s)",
                        chunk_num, chunk_num, total_chunks, view_name, f"{rows_expected:,}", f"{rows_returned:,}", f"{missing_rows:,}",
                    )
                    retry_skip = skip + rows_returned
                    retry_top = missing_rows
                    logger.info("[SAP Fetch] 🔄 Step 3.%s Refetch: Calling API again for remaining %s rows (skip=%s, top=%s)", chunk_num, f"{retry_top:,}", f"{retry_skip:,}", f"{retry_top:,}")
                    try:
                        remainder_result = await datasphere_service.execute_odata_query(
                            user_id=user_id,
                            view_name=view_name,
                            select=select,
                            filter=filter_expr,
                            top=retry_top,
                            skip=retry_skip,
                            orderby=orderby,
                            data_url=data_url,
                            space_id=space_id,
                            token=token,
                            total_count=total_count,
                        )
                        remainder_rows = len(remainder_result.data) if remainder_result.data else 0
                        if remainder_rows > 0:
                            combined_data = result.data + remainder_result.data
                            logger.info("[SAP Fetch] ✅ Step 3.%s Refetch complete: Got %s remaining rows, combined total %s rows for chunk %s", chunk_num, f"{remainder_rows:,}", f"{len(combined_data):,}", chunk_num)
                            try:
                                chunk_lf = pl.LazyFrame(combined_data, infer_schema_length=None)
                                chunk_lf.collect_schema()
                            except Exception:
                                chunk_df = pl.DataFrame(combined_data, infer_schema_length=None)
                                chunk_lf = chunk_df.lazy()
                            rows_returned = len(combined_data)
                        else:
                            logger.warning("[SAP Fetch] ⚠️ Step 3.%s Refetch returned no rows (still missing %s)", chunk_num, f"{missing_rows:,}")
                    except Exception as refetch_err:
                        logger.error("[SAP Fetch] ❌ Step 3.%s Refetch failed: %s", chunk_num, refetch_err, exc_info=True)
                else:
                    logger.info("[SAP Fetch] ✅ Step 3.%s Complete: Chunk %s/%s for '%s': %s rows fetched and converted to LazyFrame", chunk_num, chunk_num, total_chunks, view_name, f"{rows_returned:,}")
                return (chunk_lf, rows_returned, rows_expected, api_url)
            except Exception as e:
                logger.error("[SAP Fetch] ❌ Step 3.%s Failed to create LazyFrame: %s", chunk_num, e, exc_info=True)
                return None
        else:
            logger.warning("[SAP Fetch] ⚠️ Step 3.%s Warning: Chunk %s/%s for '%s': No data returned", chunk_num, chunk_num, total_chunks, view_name)
            return (pl.LazyFrame(), 0, rows_expected, None)
    except Exception as e:
        logger.error("[SAP Fetch] ❌ Step 3.%s Failed: Chunk %s/%s for '%s': %s", chunk_num, chunk_num, total_chunks, view_name, e)
        return None


async def fetch_view_data(
    datasphere_service: Any,
    user_id: str,
    view_name: str,
    filter_expr: Optional[str],
    orderby: Optional[str],
    data_url: Optional[str],
    space_id: Optional[str],
    token: str,
    sap_view_schemas: Optional[Dict[str, Any]] = None,
    select: Optional[str] = None,
) -> Tuple[Optional[pl.LazyFrame], int, Optional[str], Dict[str, Any]]:
    """Fetch all data for a relational view using count-first approach with $orderby.

    Returns:
        (lazy_frame, total_api_calls, api_url, fetch_status)
    """
    logger.info("[SAP Fetch] ========== Starting fetch for '%s' ==========", view_name)
    select = clean_odata_select(select)

    date_columns: List[str] = []
    if sap_view_schemas:
        date_columns = extract_date_columns_from_schema(view_name, sap_view_schemas)
    if date_columns:
        orderby = date_columns[0]
        logger.info("[SAP Fetch] 📋 Using date column '%s' for $orderby on '%s' (%s date column(s) found)", orderby, view_name, len(date_columns))
    elif orderby:
        logger.info("[SAP Fetch] 📋 Using provided orderby '%s' for '%s' (no date columns found in schema)", orderby, view_name)
    else:
        logger.warning("[SAP Fetch] ⚠️ WARNING: No $orderby column for '%s' and no date columns found in schema", view_name)
    if not orderby:
        logger.warning("[SAP Fetch] ⚠️ WARNING: No $orderby for '%s' - data may not be consistently ordered", view_name)

    total_count = await _get_row_count(datasphere_service, user_id, view_name, filter_expr, orderby, data_url, space_id, token, select=select)
    if total_count is None:
        fetch_status = {"planned_rows": 0, "actual_rows": 0, "failed_chunks": 0, "total_chunks": 0, "message": "SAP failed to return data for this view (e.g. count call returned server error)."}
        return None, 0, None, fetch_status
    if total_count == 0:
        fetch_status = {"planned_rows": 0, "actual_rows": 0, "failed_chunks": 0, "total_chunks": 0, "message": None}
        return pl.LazyFrame(), 1, None, fetch_status

    rows_to_fetch = total_count
    total_chunks = (rows_to_fetch + MAX_ROWS_PER_PAGE - 1) // MAX_ROWS_PER_PAGE
    logger.info("[SAP Fetch] ✅ Step 2 Complete: '%s' - Count=%s, will fetch ALL %s rows in %s chunk(s) using $top=%s and $skip", view_name, f"{total_count:,}", f"{rows_to_fetch:,}", total_chunks, f"{MAX_ROWS_PER_PAGE:,}")

    chunk_lazy_frames: List[pl.LazyFrame] = []
    failed_chunks = 0
    successful_chunks = 0
    total_rows_collected = 0
    api_url = None

    all_chunk_params: List[Tuple[int, int, int]] = []
    current_skip = 0
    remaining_rows = rows_to_fetch
    for chunk_num in range(1, total_chunks + 1):
        if remaining_rows <= 0:
            break
        if len(all_chunk_params) >= MAX_CHUNK_CALLS_PER_VIEW:
            logger.warning("[SAP Fetch] ⚠️ Capping at %s chunk API calls per view. Total chunks needed was %s.", MAX_CHUNK_CALLS_PER_VIEW, total_chunks)
            break
        top = min(MAX_ROWS_PER_PAGE, remaining_rows)
        if top <= 0:
            break
        all_chunk_params.append((chunk_num, current_skip, top))
        current_skip += MAX_ROWS_PER_PAGE
        remaining_rows -= top

    if len(all_chunk_params) < total_chunks:
        rows_to_fetch = sum(t for (_, _, t) in all_chunk_params)
        logger.info("[SAP Fetch] 📊 Capped fetch: requesting %s rows in %s chunks (full count was %s in %s chunks)", f"{rows_to_fetch:,}", len(all_chunk_params), f"{total_count:,}", total_chunks)

    if all_chunk_params:
        max_concurrent = getattr(settings, "sap_batch_concurrency", 50)
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_chunk_with_semaphore(cnum: int, sk: int, tp: int) -> Optional[Tuple]:
            async with semaphore:
                return await _fetch_data_chunk(
                    datasphere_service, user_id, view_name, filter_expr, orderby,
                    data_url, space_id, token, tp, sk, cnum, total_chunks, total_count, select=select,
                )

        chunk_tasks = [fetch_chunk_with_semaphore(cnum, sk, tp) for cnum, sk, tp in all_chunk_params]
        logger.info("[SAP Fetch] 🚀 Step 3.2: Starting parallel fetch of %s chunk(s) (max %s concurrent)...", len(chunk_tasks), max_concurrent)
        try:
            chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
            for idx, (chunk_num, skip, top) in enumerate(all_chunk_params):
                result = chunk_results[idx]
                if result is None:
                    logger.error("[SAP Fetch] ❌ Chunk %s failed (returned None)", chunk_num)
                    failed_chunks += 1
                elif isinstance(result, Exception):
                    logger.error("[SAP Fetch] ❌ Chunk %s failed with exception: %s", chunk_num, result)
                    failed_chunks += 1
                else:
                    if len(result) == 4:
                        chunk_lf, rows_returned, rows_expected, chunk_api_url = result
                        if api_url is None and chunk_api_url:
                            api_url = chunk_api_url
                    else:
                        chunk_lf, rows_returned, rows_expected = result[:3]
                    if chunk_lf is not None and rows_returned > 0:
                        chunk_lazy_frames.append(chunk_lf)
                        total_rows_collected += rows_returned
                    successful_chunks += 1
        except Exception as e:
            logger.error("[SAP Fetch] ❌ Error during parallel chunk fetching: %s", e, exc_info=True)
    else:
        logger.info("[SAP Fetch] ✅ No chunks to fetch (count is 0 or no chunk params calculated)")

    total_chunks_fetched = len(all_chunk_params) if all_chunk_params else 0
    fetch_status = {
        "planned_rows": rows_to_fetch,
        "actual_rows": total_rows_collected,
        "failed_chunks": failed_chunks,
        "total_chunks": total_chunks_fetched,
        "message": None,
    }
    if failed_chunks > 0 or total_rows_collected < rows_to_fetch:
        missing = rows_to_fetch - total_rows_collected
        fetch_status["message"] = (
            f"SAP was unable to return some of the requested data (e.g. server errors such as 500). "
            f"Planned {rows_to_fetch:,} rows; retrieved {total_rows_collected:,} (missing {missing:,} from {failed_chunks} failed chunk(s))."
        )

    if not chunk_lazy_frames:
        logger.warning("[SAP Fetch] ⚠️ '%s': No data collected from any chunks", view_name)
        return pl.LazyFrame(), 1 + total_chunks_fetched if all_chunk_params else 1, None, fetch_status

    # Step 5: Combine LazyFrames with schema unification
    try:
        if len(chunk_lazy_frames) == 1:
            combined_lf = chunk_lazy_frames[0]
        else:
            try:
                all_schemas = []
                for i, chunk_lf in enumerate(chunk_lazy_frames):
                    try:
                        all_schemas.append(chunk_lf.collect_schema())
                    except Exception as e:
                        logger.warning("[SAP Fetch] ⚠️ Could not get schema for chunk %s: %s", i + 1, e)
                if not all_schemas:
                    return pl.LazyFrame(), 1 + len(all_chunk_params) if all_chunk_params else 1, None, fetch_status
                all_column_names = set()
                for schema in all_schemas:
                    all_column_names.update(schema.keys())
                type_order = {
                    pl.Null: 0, pl.Boolean: 1, pl.Int8: 1, pl.Int16: 1, pl.Int32: 1, pl.Int64: 1,
                    pl.UInt8: 1, pl.UInt16: 1, pl.UInt32: 1, pl.UInt64: 1,
                    pl.Float32: 2, pl.Float64: 2, pl.Date: 1, pl.Datetime: 1, pl.String: 3,
                }
                unified_schema = {}
                for col_name in all_column_names:
                    widest_type = None
                    widest_order = -1
                    for schema in all_schemas:
                        if col_name in schema:
                            col_type = schema[col_name]
                            type_priority = type_order.get(col_type, 2)
                            if type_priority > widest_order:
                                widest_order = type_priority
                                widest_type = col_type
                            elif type_priority == widest_order and widest_type is None:
                                widest_type = col_type
                    unified_schema[col_name] = widest_type if widest_type is not None else pl.String
                unified_chunks = []
                for i, chunk_lf in enumerate(chunk_lazy_frames):
                    try:
                        chunk_schema = chunk_lf.collect_schema()
                        select_exprs = []
                        for col_name, target_type in unified_schema.items():
                            if col_name in chunk_schema:
                                current_type = chunk_schema[col_name]
                                if current_type != target_type:
                                    select_exprs.append(pl.col(col_name).cast(target_type, strict=False))
                                else:
                                    select_exprs.append(pl.col(col_name))
                            else:
                                select_exprs.append(pl.lit(None, dtype=target_type).alias(col_name))
                        unified_chunks.append(chunk_lf.select(select_exprs))
                    except Exception as e:
                        logger.warning("[SAP Fetch] ⚠️ Failed to unify chunk %s schema: %s", i + 1, e)
                        unified_chunks.append(chunk_lf)
                combined_lf = pl.concat(unified_chunks, how="vertical")
            except Exception as schema_error:
                logger.error("[SAP Fetch] ❌ Schema unification failed: %s", schema_error, exc_info=True)
                logger.warning("[SAP Fetch] ⚠️ Falling back to collecting all chunks to handle schema mismatch")
                try:
                    all_dataframes = [chunk_lf.collect() for chunk_lf in chunk_lazy_frames]
                    if all_dataframes:
                        combined_lf = pl.concat(all_dataframes, how="diagonal").lazy()
                    else:
                        return pl.LazyFrame(), 1 + len(all_chunk_params) if all_chunk_params else 1, None, fetch_status
                except Exception as e2:
                    logger.error("[SAP Fetch] ❌ All concat methods failed: %s", e2, exc_info=True)
                    err_status = {**fetch_status, "message": f"Concat failed: {str(e2)[:200]}"}
                    return None, 0, None, err_status
        logger.info("[SAP Fetch] ========== Fetch complete for '%s' ==========", view_name)
        total_api_calls = 1 + len(all_chunk_params) if all_chunk_params else 1
        return combined_lf, total_api_calls, api_url, fetch_status
    except Exception as e:
        logger.error("[SAP Fetch] ❌ Step 5 Failed: Cannot combine LazyFrames for '%s': %s", view_name, e, exc_info=True)
        err_status = {**fetch_status, "message": f"Combine LazyFrames failed: {str(e)[:200]}"}
        return None, 0, None, err_status
