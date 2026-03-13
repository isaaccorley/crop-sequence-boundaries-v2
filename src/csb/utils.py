"""Utilities — vector ops, zonal stats, and parallelism helpers."""

from __future__ import annotations

import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import sedonadb
from contourrs import shapes_arrow
from exactextract import exact_extract

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import geopandas as gpd
    import numpy as np
    from sedonadb.context import SedonaContext

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vector geometry operations
# ---------------------------------------------------------------------------


def polygonize(
    data: np.ndarray,
    mask: np.ndarray | None = None,
    transform: object | None = None,
    connectivity: int = 4,
    nodata: int | None = 0,
) -> pa.Table:
    """Convert a raster to polygon geometries as an Arrow table.

    Uses contourrs.shapes_arrow for zero-copy Rust→Arrow polygonization.

    Args:
        data: 2D integer array of raster values.
        mask: Optional boolean mask (True = valid pixels).
        transform: Affine transform for georeferencing.
        connectivity: Pixel connectivity (4 or 8).
        nodata: Value to exclude from polygonization.

    Returns:
        PyArrow Table with 'geometry' (WKB) and 'value' (float64) columns.
    """
    return shapes_arrow(
        data,
        mask=mask,
        connectivity=connectivity,
        transform=transform,
        nodata=nodata,
    )


def make_sedona() -> SedonaContext:
    """Create a fresh SedonaDB context."""
    return sedonadb.connect()


def eliminate_small_polygons(
    table: pa.Table,
    thresholds: list[float],
    sd: SedonaContext,
) -> pa.Table:
    """Iteratively merge small polygons into the neighbor with the longest shared boundary.

    Mirrors arcpy.management.Eliminate(selection="LENGTH"). For each threshold
    (ascending), polygons with area <= threshold are dissolved into the neighbor
    that shares the longest boundary segment.

    SQL implementation via SedonaDB — no Shapely/copy overhead.

    Args:
        table: Arrow table with 'geometry' (WKB binary) and 'effective_count' columns.
        thresholds: Area thresholds in ascending order (sq meters).
        sd: SedonaDB context (from sedonadb.connect()).

    Returns:
        Arrow table with the same schema after elimination.
    """
    # Strip geoarrow extension metadata so SedonaDB accepts ST_GeomFromWKB
    geom_idx = table.schema.get_field_index("geometry")
    table = table.set_column(geom_idx, "geometry", table.column("geometry").cast(pa.binary()))

    # Stable row-id required for self-join
    table = table.append_column("_rid", pa.array(range(table.num_rows), pa.int64()))

    for threshold in thresholds:
        if table.num_rows == 0:
            break

        sd.create_data_frame(table).to_view("_elim", overwrite=True)
        result = pa.RecordBatchReader.from_stream(
            sd.sql(f"""
                WITH
                src AS (
                    SELECT _rid,
                           ST_GeomFromWKB(geometry) AS geom,
                           effective_count,
                           ST_Area(ST_GeomFromWKB(geometry)) AS area_sqm
                    FROM _elim
                ),
                small AS (SELECT _rid, geom FROM src WHERE area_sqm <= {threshold}),
                large AS (SELECT _rid, geom, effective_count FROM src WHERE area_sqm > {threshold}),
                -- touching (small, large) pairs with shared boundary length
                pairs AS (
                    SELECT
                        s._rid AS small_rid,
                        l._rid AS large_rid,
                        ST_Length(
                            ST_Intersection(ST_Boundary(s.geom), ST_Boundary(l.geom))
                        ) AS shared_len
                    FROM small s, large l
                    WHERE ST_Touches(s.geom, l.geom)
                ),
                -- best large neighbor per small (longest shared boundary)
                best AS (
                    SELECT small_rid, large_rid
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY small_rid ORDER BY shared_len DESC
                               ) AS rn
                        FROM pairs
                    ) t
                    WHERE rn = 1
                ),
                -- all pieces to union per merge target: the large + its assigned smalls
                pieces AS (
                    SELECT b.large_rid, s.geom FROM best b JOIN small s ON s._rid = b.small_rid
                    UNION ALL
                    SELECT b.large_rid, l.geom FROM best b JOIN large l ON l._rid = b.large_rid
                ),
                -- union each group and repair geometry
                merged AS (
                    SELECT large_rid AS _rid,
                           ST_MakeValid(ST_Union_Agg(geom)) AS geom
                    FROM pieces
                    GROUP BY large_rid
                ),
                -- large polygons with no assigned smalls pass through unchanged
                untouched AS (
                    SELECT _rid, geom FROM large
                    WHERE _rid NOT IN (SELECT large_rid FROM best)
                ),
                combined AS (
                    SELECT _rid, geom FROM merged
                    UNION ALL
                    SELECT _rid, geom FROM untouched
                )
                -- restore effective_count from the large polygon record
                SELECT c._rid, ST_AsWKB(c.geom) AS geometry, l.effective_count
                FROM combined c
                JOIN large l ON l._rid = c._rid
            """)
        ).read_all()
        # Cast geometry from binary_view → binary for DuckDB compatibility downstream
        geom_idx = result.schema.get_field_index("geometry")
        result = result.set_column(
            geom_idx, "geometry", result.column("geometry").cast(pa.binary())
        )
        table = result

    rid_idx = table.schema.get_field_index("_rid")
    if rid_idx >= 0:
        table = table.remove_column(rid_idx)
    return table


# ---------------------------------------------------------------------------
# Zonal statistics
# ---------------------------------------------------------------------------


def zonal_majority(
    zones: gpd.GeoDataFrame,
    zone_id_field: str,
    value_raster_path: str | Path,
) -> dict[int, int]:
    """Compute the majority value from a raster within each zone polygon.

    Args:
        zones: GeoDataFrame with polygon geometries and a zone ID column.
        zone_id_field: Column name in zones containing integer zone IDs.
        value_raster_path: Path to the raster whose values are summarized.

    Returns:
        Dict mapping zone_id -> majority value.
    """
    results: Any = exact_extract(
        str(value_raster_path),
        zones,
        ["majority"],
        include_cols=[zone_id_field],
        output="pandas",
    )

    # Vectorized: direct column access instead of iterrows
    valid = results["majority"].notna()
    ids = results.loc[valid, zone_id_field].astype(int)
    vals = results.loc[valid, "majority"].astype(int)
    return dict(zip(ids, vals, strict=True))


# ---------------------------------------------------------------------------
# Parallelism helpers
# ---------------------------------------------------------------------------


def worker_count(cpu_fraction: float = 0.90) -> int:
    """Return number of worker processes based on CPU fraction."""
    total = multiprocessing.cpu_count()
    return max(1, round(cpu_fraction * total))


def parallel_map(
    fn: Callable[..., Any],
    items: list[Any],
    max_workers: int | None = None,
    desc: str = "Processing",
    show_progress: bool = True,
) -> list[Any]:
    """Map fn over items using ProcessPoolExecutor with Rich progress bar.

    Results are returned in submission order.
    """
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )

    max_workers = max_workers or worker_count()
    logger.info("Running %s tasks across %s workers", len(items), max_workers)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("eta"),
        TimeRemainingColumn(),
        disable=not show_progress,
    )

    results: list[Any] = [None] * len(items)
    with progress:
        task_id = progress.add_task(desc, total=len(items))
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(fn, item): i for i, item in enumerate(items)}
            for future in futures:
                idx = futures[future]
                results[idx] = future.result()
                progress.advance(task_id)

    return results


def parallel_starmap(
    fn: Callable[..., Any],
    items: list[tuple[Any, ...]],
    max_workers: int | None = None,
    desc: str = "Processing",
    show_progress: bool = True,
) -> list[Any]:
    """Like parallel_map but unpacks tuple args via starmap."""
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )

    max_workers = max_workers or worker_count()
    logger.info("Running %s tasks across %s workers", len(items), max_workers)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("eta"),
        TimeRemainingColumn(),
        disable=not show_progress,
    )

    results: list[Any] = [None] * len(items)
    with progress:
        task_id = progress.add_task(desc, total=len(items))
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(fn, *args): i for i, args in enumerate(items)}
            for future in futures:
                idx = futures[future]
                results[idx] = future.result()
                progress.advance(task_id)

    return results
