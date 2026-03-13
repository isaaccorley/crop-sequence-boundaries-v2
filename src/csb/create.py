"""Stage 1: CREATE — Combine multi-year CDL rasters into crop sequence boundary polygons.

Mirrors the USDA original algorithm (ArcGIS Combine):

Per window tile (embarrassingly parallel):
1. Windowed-read multi-year CDL rasters from national files (no split stage needed)
2. Encode each pixel's N-year CDL sequence as a packed int64; assign compact combo IDs
3. Compute COUNT0 / COUNT45 per unique combo (not per pixel)
4. Polygonize the combo-ID raster using contourrs (Rust → Arrow, zero-copy)
5. Join effective_count per combo; filter: effective >= min_cropland OR
   (area >= 10000 AND effective >= 1)  [USDA two-threshold rule]
6. Eliminate small polygons by merging into longest-shared-boundary neighbor
7. Simplify + min-area filter in DuckDB (ST_SimplifyPreserveTopology)
8. Write to GeoParquet
"""

from __future__ import annotations

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
    stacks: list[np.ndarray] = []
    transform = None
    for year in years:
        cdl_path = national_cdl / str(year) / f"{year}_30m_cdls.tif"
        arr, t = _read_window(cdl_path, window)
        # Reclassify non-cropland pixels (CDL > CDL_CROP_MAX, i.e. water/developed/forest/
        # grassland/wetlands) to BARREN_CODE so they don't contribute to effective_count.
        # CDL 0 (no-data) is left as 0.  Cropland classes 1–CDL_CROP_MAX are kept as-is.
        arr = arr.astype(np.int64)
        arr = np.where((arr > CDL_CROP_MAX) & (arr != 0), BARREN_CODE, arr)
        stacks.append(arr)
        if transform is None:
            transform = t

    # Encode each pixel's annual sequence as a single packed int64.
    # CDL values are 0-255, so base-256 packing is lossless for up to ~8 years.
    # For longer spans we use base-300 to be safe.
    shape = stacks[0].shape
    base = np.int64(300)
    seq_ids = np.zeros(shape, dtype=np.int64)
    for i, arr in enumerate(stacks):
        seq_ids += arr * (base**i)
    del stacks  # free ~N×200MB per worker

    # Assign compact sequential IDs (0, 1, 2, ...) to each unique sequence
    unique_seqs, flat_ids = np.unique(seq_ids.ravel(), return_inverse=True)
    del seq_ids  # free ~200MB
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


def process_area(args: tuple[str, dict[str, Any]]) -> str:
    """Process a single window tile through the full CREATE pipeline.

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
    logger.info(f"{area}: Reading {len(years)} years (windowed)")
    combo_raster, effective_per_combo, transform = _combine_years_windowed(
        national_cdl, years, window
    )

    # 2. Mask pixels where effective_count >= 1 for any combo, then polygonize combo IDs
    effective_map = effective_per_combo[combo_raster]  # broadcast to 2D
    mask = effective_map >= 1
    if not mask.any():
        return f"Skipped {area} (no valid pixels)"

    logger.info(f"{area}: Polygonizing {int(mask.sum())} valid pixels")
    table = polygonize(
        combo_raster,
        mask=mask,
        transform=transform,
        nodata=-1,
    )

    if table.num_rows == 0:
        return f"Skipped {area} (no polygons)"

    # 3. Join effective_count per combo; apply USDA two-threshold filter:
    #    keep if effective >= min_cropland  OR  (area >= 10000 AND effective >= 1)
    #    Also compute area once and pre-drop sub-threshold polygons that cannot
    #    survive eliminate (area < first threshold) to shrink the eliminate input.
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
    # Drop single-pixel noise (area == CDL_PIXEL_AREA_SQM) before eliminate;
    # these sub-threshold polygons add no meaningful contribution and are just
    # CDL classification noise.  Use strict > so 2-pixel polygons are kept.
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

    logger.info(f"{area}: Eliminating small polygons ({filtered.num_rows} input)")
    geoms, vals = eliminate_small_polygons(geoms, vals, thresholds)

    if not geoms:
        conn.close()
        return f"Skipped {area} (all eliminated)"

    # 5. Simplify + min-area filter in DuckDB
    logger.info(f"{area}: Simplifying {len(geoms)} polygons (DuckDB)")
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
    logger.info(f"{area}: Done — {out_table.num_rows} polygons in {elapsed:.2f} min")
    return f"Finished {area} ({out_table.num_rows} polygons, {elapsed:.1f} min)"


def run_create(
    cfg: dict[str, Any],
    start_year: int,
    end_year: int,
    output_dir: str | Path,
    area: str | None = None,
) -> Path:
    """Run the CREATE stage for all (or one) window tile(s).

    Reads directly from national CDL rasters via windowed reads — no split stage needed.

    Args:
        cfg: Loaded config dict.
        start_year: First CDL year.
        end_year: Last CDL year (inclusive).
        output_dir: Directory for output parquets.
        area: Optional single area tile to process.

    Returns:
        Path to the output directory containing parquet files.
    """
    from rich.console import Console

    from csb.utils import parallel_map, worker_count

    console = Console()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    national_cdl = Path(cfg["paths"]["national_cdl"])
    tile_size = cfg.get("create", {}).get("tile_size", DEFAULT_TILE_SIZE)

    # Discover raster dimensions from the first year
    first_cdl = national_cdl / str(start_year) / f"{start_year}_30m_cdls.tif"
    with rasterio.open(first_cdl) as src:
        raster_width, raster_height = src.width, src.height

    # Generate windows
    all_tiles = _tile_windows(raster_width, raster_height, tile_size)
    if area:
        all_tiles = [(name, win) for name, win in all_tiles if name == area]

    tile_names = [name for name, _ in all_tiles]

    # Skip already-done areas
    done = {f.stem for f in output_dir.glob("*.parquet")}
    remaining = [(name, win) for name, win in all_tiles if name not in done]

    n_workers = worker_count(cfg["global"]["cpu_fraction"])
    console.print(
        f"CREATE: {len(remaining)}/{len(tile_names)} tiles, "
        f"{start_year}-{end_year}, {n_workers} workers"
    )

    if not remaining:
        console.print("[green]All tiles already processed.")
        return output_dir

    params = {
        "config": cfg,
        "start_year": start_year,
        "end_year": end_year,
        "output_dir": str(output_dir),
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
        for name, w in remaining
    ]
    results = parallel_map(
        process_area,
        task_args,
        max_workers=n_workers,
        desc=f"CREATE {start_year}-{end_year}",
    )

    # Summary: count finished vs skipped
    finished = [r for r in results if r.startswith("Finished")]
    skipped = [r for r in results if r.startswith("Skipped")]
    total_polys = sum(int(r.split("(")[1].split()[0]) for r in finished if "polygons" in r)
    console.print(
        f"[bold green]CREATE complete: {len(finished)} tiles, "
        f"{total_polys:,} polygons ({len(skipped)} skipped)"
    )
    return output_dir
