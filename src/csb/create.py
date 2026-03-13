"""Stage 1: CREATE — Combine multi-year CDL rasters into crop sequence boundary polygons.

Mirrors the USDA original algorithm (ArcGIS Combine):

Per window tile, split into two phases for memory efficiency:

Phase 1 (memory-heavy, few workers):
1. Windowed-read multi-year CDL rasters from national files
2. Encode each pixel's N-year CDL sequence as a packed int64; assign compact combo IDs
3. Compute COUNT0 / COUNT45 per unique combo (not per pixel)
4. Polygonize the combo-ID raster using contourrs (Rust -> Arrow, zero-copy)
5. Join effective_count per combo; filter with USDA two-threshold rule
6. Write intermediate GeoParquet

Phase 2 (CPU-bound, many workers):
7. Eliminate small polygons by merging into longest-shared-boundary neighbor
8. Simplify + min-area filter in DuckDB (ST_SimplifyPreserveTopology)
9. Write final GeoParquet
"""

from __future__ import annotations

import gc
import logging
import time
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pyarrow as pa
import rasterio
import rasterio.windows
from rasterio.windows import Window

from csb.config import BARREN_CODE, CDL_CROP_MAX, CDL_PIXEL_AREA_SQM
from csb.io import write_geoparquet
from csb.utils import (
    arrow_to_geometries,
    eliminate_small_polygons,
    geometries_to_arrow,
    polygonize,
)

logger = logging.getLogger(__name__)

# Default tile size in pixels (5000 x 5000 @ 30m = 150km x 150km)
DEFAULT_TILE_SIZE = 5000


def _tile_windows(width: int, height: int, tile_size: int) -> list[tuple[str, Window]]:
    """Generate named tile windows covering the full raster extent.

    Names follow a grid pattern: row letter (A-Z, AA-AZ, ...) + column number.
    """
    tiles = []
    for row_idx, y_off in enumerate(range(0, height, tile_size)):
        h = min(tile_size, height - y_off)
        if row_idx < 26:
            row_label = chr(65 + row_idx)
        else:
            row_label = chr(65 + row_idx // 26 - 1) + chr(65 + row_idx % 26)
        for col_idx, x_off in enumerate(range(0, width, tile_size)):
            w = min(tile_size, width - x_off)
            name = f"{row_label}{col_idx}"
            window = Window(x_off, y_off, w, h)  # type: ignore[call-arg]
            tiles.append((name, window))
    return tiles


def _read_window(cdl_path: Path, window: Window) -> tuple[np.ndarray, Any]:
    """Read a window from a CDL raster, returning (2D array, transform)."""
    with rasterio.open(cdl_path) as src:
        data = src.read(1, window=window)
        transform = rasterio.windows.transform(window, src.transform)
    return data, transform


def _combine_years_windowed(
    national_cdl: Path, years: list[int], window: Window
) -> tuple[np.ndarray, np.ndarray, Any]:
    """Read the same window from each year's CDL; assign unique combo IDs per pixel sequence.

    Mirrors ArcGIS Combine: groups pixels by their full N-year CDL value sequence,
    not by scalar counts. Each unique sequence gets a compact integer ID.

    Returns:
        combo_raster: 2D int32 array of compact combo IDs (0-based)
        effective_per_combo: 1D int16 array — (COUNT0 - COUNT45) for each combo ID
        transform: Affine transform for this window
    """
    # Incrementally pack each year's CDL into seq_ids to avoid holding all
    # N raster arrays in memory simultaneously.
    base = np.int64(300)
    seq_ids: np.ndarray | None = None
    transform = None
    for i, year in enumerate(years):
        cdl_path = national_cdl / str(year) / f"{year}_30m_cdls.tif"
        arr, t = _read_window(cdl_path, window)
        if transform is None:
            transform = t
        # Reclassify non-cropland pixels (CDL > CDL_CROP_MAX) to BARREN_CODE.
        arr = arr.astype(np.int64)
        arr = np.where((arr > CDL_CROP_MAX) & (arr != 0), BARREN_CODE, arr)
        if seq_ids is None:
            seq_ids = arr  # first year: just the array itself
        else:
            seq_ids += arr * (base**i)

    assert seq_ids is not None
    shape = seq_ids.shape

    # Assign compact sequential IDs (0, 1, 2, ...) to each unique sequence
    unique_seqs, flat_ids = np.unique(seq_ids.ravel(), return_inverse=True)
    del seq_ids
    combo_raster = flat_ids.reshape(shape).astype(np.int32)
    del flat_ids

    # Compute effective_count per unique sequence
    n_combos = len(unique_seqs)
    count0 = np.zeros(n_combos, dtype=np.int16)
    count45 = np.zeros(n_combos, dtype=np.int16)
    for i in range(len(years)):
        yr_vals = (unique_seqs // int(base**i)) % int(base)
        count0 += (yr_vals > 0).astype(np.int16)
        count45 += (yr_vals == BARREN_CODE).astype(np.int16)

    effective_per_combo = (count0 - count45).astype(np.int16)
    return combo_raster, effective_per_combo, transform


# ---------------------------------------------------------------------------
# Phase 1: combine + polygonize + DuckDB filter (memory-heavy)
# ---------------------------------------------------------------------------


def _phase1_polygonize(args: tuple[str, dict[str, Any]]) -> str:
    """Phase 1: Read CDL windows, combine, polygonize, filter -> intermediate parquet.

    This is the memory-heavy phase (raster I/O + polygonization).
    """
    area, params = args
    cfg = params["config"]
    start_year: int = params["start_year"]
    end_year: int = params["end_year"]
    intermediate_dir = Path(params["intermediate_dir"])
    window_dict = params["window"]
    window = Window(**window_dict)

    national_cdl = Path(cfg["paths"]["national_cdl"])
    min_cropland = cfg["global"]["min_cropland_years"]

    years = list(range(start_year, end_year + 1))
    t0 = time.perf_counter()

    # 1. Read windows directly from national CDL
    logger.info("%s: Phase 1 - Reading %s years (windowed)", area, len(years))
    combo_raster, effective_per_combo, transform = _combine_years_windowed(
        national_cdl, years, window
    )

    # 2. Mask pixels where effective_count >= 1 for any combo, then polygonize
    effective_map = effective_per_combo[combo_raster]
    mask = effective_map >= 1
    if not mask.any():
        return f"Skipped {area} (no valid pixels)"

    logger.info("%s: Polygonizing %s valid pixels", area, int(mask.sum()))
    table = polygonize(
        combo_raster,
        mask=mask,
        transform=transform,
        nodata=-1,
    )
    del combo_raster, effective_map, mask
    gc.collect()

    if table.num_rows == 0:
        return f"Skipped {area} (no polygons)"

    # 3. DuckDB filter: USDA two-threshold + single-pixel noise removal
    combo_table = pa.table(
        {
            "combo_id": pa.array(
                np.arange(len(effective_per_combo), dtype=np.int32), type=pa.int32()
            ),
            "effective_count": pa.array(effective_per_combo.astype(np.int32), type=pa.int32()),
        }
    )

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.register("polys", table)
    conn.register("combos", combo_table)
    filtered = (
        conn.execute(f"""
        SELECT p.geometry, c.effective_count,
               ST_Area(ST_GeomFromWKB(p.geometry::BLOB)) AS area_sqm
        FROM polys p
        JOIN combos c ON CAST(p.value AS INTEGER) = c.combo_id
        WHERE (c.effective_count >= {min_cropland}
               OR (ST_Area(ST_GeomFromWKB(p.geometry::BLOB)) >= 10000 AND c.effective_count >= 1))
          AND ST_Area(ST_GeomFromWKB(p.geometry::BLOB)) > {CDL_PIXEL_AREA_SQM}
    """)
        .arrow()
        .read_all()
    )
    conn.close()
    del table, combo_table
    gc.collect()

    if filtered.num_rows == 0:
        return f"Skipped {area} (all filtered)"

    # 4. Write intermediate parquet
    out_path = intermediate_dir / f"{area}.parquet"
    write_geoparquet(filtered, out_path)

    elapsed = time.perf_counter() - t0
    logger.info("%s: Phase 1 done - %s polygons in %.0fs", area, filtered.num_rows, elapsed)
    return f"Phase1 {area} ({filtered.num_rows} polygons, {elapsed:.0f}s)"


# ---------------------------------------------------------------------------
# Phase 2: eliminate + simplify (CPU-bound, low memory)
# ---------------------------------------------------------------------------


def _phase2_eliminate(args: tuple[str, dict[str, Any]]) -> str:
    """Phase 2: Eliminate small polygons + simplify -> final parquet.

    This is CPU-bound but uses much less memory than phase 1.
    """
    area, params = args
    cfg = params["config"]
    intermediate_dir = Path(params["intermediate_dir"])
    output_dir = Path(params["output_dir"])

    thresholds = cfg["create"]["eliminate_thresholds"]
    min_area = cfg["create"]["min_polygon_area"]
    simplify_tol = cfg["create"]["simplify_tolerance"]

    intermediate_path = intermediate_dir / f"{area}.parquet"
    t0 = time.perf_counter()

    # 1. Read intermediate parquet
    import pyarrow.parquet as pq

    filtered = pq.read_table(intermediate_path)

    # 2. Eliminate small polygons
    geoms = arrow_to_geometries(filtered)
    vals = filtered.column("effective_count").to_pylist()
    del filtered

    logger.info("%s: Phase 2 - Eliminating small polygons (%s input)", area, len(geoms))
    geoms, vals = eliminate_small_polygons(geoms, vals, thresholds)

    if not geoms:
        return f"Skipped {area} (all eliminated)"

    # 3. Simplify + min-area filter in DuckDB
    logger.info("%s: Simplifying %s polygons (DuckDB)", area, len(geoms))
    intermediate = geometries_to_arrow(geoms, columns={"effective_count": vals})
    del geoms, vals

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.register("elim", intermediate)
    out_table = (
        conn.execute(f"""
        WITH simplified AS (
            SELECT
                ST_SimplifyPreserveTopology(ST_GeomFromWKB(geometry), {simplify_tol}) AS geometry,
                effective_count
            FROM elim
        )
        SELECT geometry, effective_count, ST_Area(geometry) AS area_sqm
        FROM simplified
        WHERE ST_Area(geometry) >= {min_area}
    """)
        .arrow()
        .read_all()
    )
    conn.close()

    if out_table.num_rows == 0:
        return f"Skipped {area} (all below min area)"

    # 4. Write final GeoParquet
    out_path = output_dir / f"{area}.parquet"
    write_geoparquet(out_table, out_path)

    elapsed = time.perf_counter() - t0
    logger.info("%s: Phase 2 done - %s polygons in %.0fs", area, out_table.num_rows, elapsed)
    return f"Finished {area} ({out_table.num_rows} polygons, {elapsed:.0f}s)"


# ---------------------------------------------------------------------------
# Legacy single-function entry point (used by tests)
# ---------------------------------------------------------------------------


def process_area(args: tuple[str, dict[str, Any]]) -> str:
    """Process a single window tile through the full CREATE pipeline (both phases).

    Args:
        args: Tuple of (area_name, params_dict). params_dict must contain
              'window' (serialized as dict), 'config', 'start_year',
              'end_year', 'output_dir'.

    Returns:
        Status string.
    """
    area, params = args
    cfg = params["config"]
    start_year: int = params["start_year"]
    end_year: int = params["end_year"]
    output_dir = Path(params["output_dir"])
    window_dict = params["window"]
    window = Window(**window_dict)

    national_cdl = Path(cfg["paths"]["national_cdl"])
    min_cropland = cfg["global"]["min_cropland_years"]
    thresholds = cfg["create"]["eliminate_thresholds"]
    min_area = cfg["create"]["min_polygon_area"]
    simplify_tol = cfg["create"]["simplify_tolerance"]

    years = list(range(start_year, end_year + 1))
    t0 = time.perf_counter()

    # 1. Read windows directly from national CDL (no split stage)
    logger.info("%s: Reading %s years (windowed)", area, len(years))
    combo_raster, effective_per_combo, transform = _combine_years_windowed(
        national_cdl, years, window
    )

    # 2. Mask pixels where effective_count >= 1 for any combo, then polygonize combo IDs
    effective_map = effective_per_combo[combo_raster]  # broadcast to 2D
    mask = effective_map >= 1
    if not mask.any():
        return f"Skipped {area} (no valid pixels)"

    logger.info("%s: Polygonizing %s valid pixels", area, int(mask.sum()))
    table = polygonize(
        combo_raster,
        mask=mask,
        transform=transform,
        nodata=-1,
    )
    del combo_raster, effective_map, mask  # free raster memory before DuckDB
    gc.collect()

    if table.num_rows == 0:
        return f"Skipped {area} (no polygons)"

    # 3. Join effective_count per combo; apply USDA two-threshold filter
    combo_table = pa.table(
        {
            "combo_id": pa.array(
                np.arange(len(effective_per_combo), dtype=np.int32), type=pa.int32()
            ),
            "effective_count": pa.array(effective_per_combo.astype(np.int32), type=pa.int32()),
        }
    )

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.register("polys", table)
    conn.register("combos", combo_table)
    filtered = (
        conn.execute(f"""
        SELECT p.geometry, c.effective_count,
               ST_Area(ST_GeomFromWKB(p.geometry::BLOB)) AS area_sqm
        FROM polys p
        JOIN combos c ON CAST(p.value AS INTEGER) = c.combo_id
        WHERE (c.effective_count >= {min_cropland}
               OR (ST_Area(ST_GeomFromWKB(p.geometry::BLOB)) >= 10000 AND c.effective_count >= 1))
          AND ST_Area(ST_GeomFromWKB(p.geometry::BLOB)) > {CDL_PIXEL_AREA_SQM}
    """)
        .arrow()
        .read_all()
    )

    if filtered.num_rows == 0:
        conn.close()
        return f"Skipped {area} (all filtered)"

    # 4. Eliminate small polygons (inherently iterative — stays in shapely)
    geoms = arrow_to_geometries(filtered)
    vals = filtered.column("effective_count").to_pylist()
    del table, combo_table, filtered  # free Arrow/DuckDB intermediates

    logger.info("%s: Eliminating small polygons (%s input)", area, len(geoms))
    geoms, vals = eliminate_small_polygons(geoms, vals, thresholds)

    if not geoms:
        conn.close()
        return f"Skipped {area} (all eliminated)"

    # 5. Simplify + min-area filter in DuckDB
    logger.info("%s: Simplifying %s polygons (DuckDB)", area, len(geoms))
    intermediate = geometries_to_arrow(geoms, columns={"effective_count": vals})
    conn.register("elim", intermediate)
    out_table = (
        conn.execute(f"""
        WITH simplified AS (
            SELECT
                ST_SimplifyPreserveTopology(ST_GeomFromWKB(geometry), {simplify_tol}) AS geometry,
                effective_count
            FROM elim
        )
        SELECT geometry, effective_count, ST_Area(geometry) AS area_sqm
        FROM simplified
        WHERE ST_Area(geometry) >= {min_area}
    """)
        .arrow()
        .read_all()
    )
    conn.close()

    if out_table.num_rows == 0:
        return f"Skipped {area} (all below min area)"

    # 6. Write GeoParquet
    out_path = output_dir / f"{area}.parquet"
    write_geoparquet(out_table, out_path)

    elapsed = (time.perf_counter() - t0) / 60
    logger.info("%s: Done - %s polygons in %.2f min", area, out_table.num_rows, elapsed)
    return f"Finished {area} ({out_table.num_rows} polygons, {elapsed:.1f} min)"


def run_create(
    cfg: dict[str, Any],
    start_year: int,
    end_year: int,
    output_dir: str | Path,
    area: str | None = None,
) -> Path:
    """Run the CREATE stage for all (or one) window tile(s).

    Uses a two-phase approach for memory efficiency:
    - Phase 1 (few workers): raster I/O + polygonize + DuckDB filter
    - Phase 2 (many workers): eliminate + simplify

    Args:
        cfg: Loaded config dict.
        start_year: First CDL year.
        end_year: Last CDL year (inclusive).
        output_dir: Directory for output parquets.
        area: Optional single area tile to process.

    Returns:
        Path to the output directory containing parquet files.
    """
    import shutil

    from rich.console import Console

    from csb.utils import parallel_map, worker_count

    console = Console()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    intermediate_dir = output_dir / "_intermediate"
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    national_cdl = Path(cfg["paths"]["national_cdl"])
    tile_size = cfg.get("create", {}).get("tile_size", DEFAULT_TILE_SIZE)
    create_cfg = cfg.get("create", {})

    # Worker counts: phase1 (memory-heavy) vs phase2 (CPU-bound)
    default_workers = worker_count(cfg["global"]["cpu_fraction"])
    phase1_workers = create_cfg.get("phase1_workers", max(1, default_workers // 4))
    phase2_workers = (
        create_cfg.get("phase2_workers") or create_cfg.get("max_workers") or default_workers
    )

    # Discover raster dimensions from the first year
    first_cdl = national_cdl / str(start_year) / f"{start_year}_30m_cdls.tif"
    with rasterio.open(first_cdl) as src:
        raster_width, raster_height = src.width, src.height

    # Generate windows
    all_tiles = _tile_windows(raster_width, raster_height, tile_size)
    if area:
        all_tiles = [(name, win) for name, win in all_tiles if name == area]

    tile_names = [name for name, _ in all_tiles]

    # Skip already-done areas (final output)
    done = {f.stem for f in output_dir.glob("*.parquet")}
    phase1_done = {f.stem for f in intermediate_dir.glob("*.parquet")}

    # Phase 1: tiles not yet in intermediate or final
    phase1_remaining = [
        (name, win) for name, win in all_tiles if name not in done and name not in phase1_done
    ]
    # Phase 2: tiles in intermediate but not in final
    phase2_pending = [
        (name, win) for name, win in all_tiles if name in phase1_done and name not in done
    ]

    console.print(
        f"CREATE: {len(tile_names)} tiles, {start_year}-{end_year}\n"
        f"  Phase 1 (polygonize): {len(phase1_remaining)} remaining, {phase1_workers} workers\n"
        f"  Phase 2 (eliminate):  {len(phase2_pending)} pending + new, {phase2_workers} workers\n"
        f"  Already done:         {len(done)}"
    )

    if not phase1_remaining and not phase2_pending:
        console.print("[green]All tiles already processed.")
        return output_dir

    # --- Phase 1: memory-heavy polygonization ---
    if phase1_remaining:
        params = {
            "config": cfg,
            "start_year": start_year,
            "end_year": end_year,
            "intermediate_dir": str(intermediate_dir),
        }
        task_args = [
            (
                name,
                {
                    **params,
                    "window": {
                        "col_off": w.col_off,
                        "row_off": w.row_off,
                        "width": w.width,
                        "height": w.height,
                    },
                },
            )
            for name, w in phase1_remaining
        ]
        p1_results = parallel_map(
            _phase1_polygonize,
            task_args,
            max_workers=phase1_workers,
            desc=f"Phase 1: polygonize {start_year}-{end_year}",
        )
        p1_completed = [r for r in p1_results if r.startswith("Phase1")]
        p1_skipped = [r for r in p1_results if r.startswith("Skipped")]
        console.print(
            f"[blue]Phase 1 complete: {len(p1_completed)} tiles polygonized, "
            f"{len(p1_skipped)} skipped"
        )

    # --- Phase 2: CPU-bound eliminate + simplify ---
    # Collect all intermediate tiles that need phase 2
    phase2_tiles = {f.stem for f in intermediate_dir.glob("*.parquet")} - done
    phase2_work = [(name, win) for name, win in all_tiles if name in phase2_tiles]

    if phase2_work:
        params2 = {
            "config": cfg,
            "intermediate_dir": str(intermediate_dir),
            "output_dir": str(output_dir),
        }
        task_args2 = [(name, params2) for name, _win in phase2_work]
        p2_results = parallel_map(
            _phase2_eliminate,
            task_args2,
            max_workers=phase2_workers,
            desc=f"Phase 2: eliminate {start_year}-{end_year}",
        )
        finished = [r for r in p2_results if r.startswith("Finished")]
        p2_skipped = [r for r in p2_results if r.startswith("Skipped")]
        total_polys = sum(int(r.split("(")[1].split()[0]) for r in finished if "polygons" in r)
        console.print(
            f"[bold green]CREATE complete: {len(finished)} tiles, "
            f"{total_polys:,} polygons ({len(p2_skipped)} skipped)"
        )

    # Clean up intermediate directory
    shutil.rmtree(intermediate_dir, ignore_errors=True)

    return output_dir
