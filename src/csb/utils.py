"""Utilities — vector ops, zonal stats, and parallelism helpers."""

from __future__ import annotations

import logging
import multiprocessing
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import shapely
from contourrs import shapes_arrow
from exactextract import exact_extract
from shapely import from_wkb, make_valid, to_wkb
from shapely.strtree import STRtree

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import geopandas as gpd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vector geometry operations
# ---------------------------------------------------------------------------


def polygonize(
    data: np.ndarray,
    mask: np.ndarray | None = None,
    transform: Any = None,
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


def eliminate_small_polygons(
    geometries: list[Any],
    values: list[Any],
    thresholds: list[float],
) -> tuple[list[Any], list[Any]]:
    """Iteratively merge small polygons into the neighbor with the longest shared boundary.

    Mirrors arcpy.management.Eliminate(selection="LENGTH"). For each threshold
    (ascending), polygons with area <= threshold are dissolved into their
    neighbor that shares the longest boundary segment.

    Uses vectorized shapely 2.x array operations to avoid per-polygon Python loops.

    Args:
        geometries: List of shapely geometries.
        values: Parallel list of values per polygon.
        thresholds: Area thresholds in ascending order (sq meters).

    Returns:
        (filtered_geometries, filtered_values) after elimination.
    """
    geoms_arr = np.array(geometries)
    vals_arr = np.array(values)

    for threshold in thresholds:
        if len(geoms_arr) == 0:
            break

        areas = shapely.area(geoms_arr)
        small_mask = areas <= threshold
        if not small_mask.any():
            continue

        small_idx = np.where(small_mask)[0]
        large_mask = ~small_mask

        tree = STRtree(geoms_arr)
        # Vectorized: query all small polygons at once → (query_pos, result_pos) pairs
        q_pos, r_pos = tree.query(geoms_arr[small_idx], predicate="touches")

        # Only keep pairs where the result is a large polygon
        large_result = large_mask[r_pos]
        q_pos = q_pos[large_result]
        r_pos = r_pos[large_result]

        if len(q_pos) == 0:
            # No mergeable small polygons; just drop them
            keep = large_mask
            geoms_arr = geoms_arr[keep]
            vals_arr = vals_arr[keep]
            continue

        # Vectorized boundary intersection lengths
        small_geom_idx = small_idx[q_pos]  # original indices of small polygons
        boundaries_small = shapely.boundary(geoms_arr[small_geom_idx])
        boundaries_large = shapely.boundary(geoms_arr[r_pos])
        shared_lengths = shapely.length(shapely.intersection(boundaries_small, boundaries_large))

        # For each small polygon, find the large neighbor with the longest shared boundary
        merge_map: dict[int, tuple[int, float]] = {}
        for qi, ri, length in zip(small_geom_idx, r_pos, shared_lengths, strict=False):
            if length > merge_map.get(qi, (-1, 0.0))[1]:
                merge_map[qi] = (int(ri), float(length))

        # Apply merges: group all smalls per large target → one unary_union each
        groups: dict[int, list[int]] = defaultdict(list)
        for small_i, (large_i, _) in merge_map.items():
            groups[large_i].append(small_i)

        for large_i, small_indices in groups.items():
            pieces = [geoms_arr[large_i]] + [geoms_arr[j] for j in small_indices]
            geoms_arr[large_i] = make_valid(shapely.unary_union(pieces))

        # Drop merged smalls and any unmerged smalls that had no valid large neighbor
        keep = np.ones(len(geoms_arr), dtype=bool)
        for small_i in merge_map:  # keys are small polygon indices
            keep[small_i] = False
        for i in small_idx:
            if i not in merge_map:
                keep[i] = False
        geoms_arr = geoms_arr[keep]
        vals_arr = vals_arr[keep]

    return list(geoms_arr), list(vals_arr)


def geometries_to_arrow(
    geometries: list[Any],
    columns: dict[str, list[Any]] | None = None,
) -> pa.Table:
    """Convert shapely geometries + optional columns to an Arrow table with WKB geometry."""
    wkb_geoms = [to_wkb(g) for g in geometries]
    data: dict[str, Any] = {"geometry": pa.array(wkb_geoms, type=pa.binary())}
    if columns:
        data.update(columns)
    return pa.table(data)


def arrow_to_geometries(table: pa.Table, column: str = "geometry") -> list[Any]:
    """Extract shapely geometries from a WKB column in an Arrow table."""
    return [from_wkb(g.as_py()) for g in table.column(column)]


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
    logger.info(f"Running {len(items)} tasks across {max_workers} workers")

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
    logger.info(f"Running {len(items)} tasks across {max_workers} workers")

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
