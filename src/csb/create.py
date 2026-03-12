"""Stage 1: CREATE — Combine multi-year CDL rasters into crop sequence boundary polygons.

Per area tile (embarrassingly parallel):
1. Load multi-year CDL rasters with rioxarray
2. Stack and compute COUNT0 (years with value > 0) and COUNT45 (years with barren=45)
3. Polygonize the effective-count raster using contourrs (Rust → Arrow, zero-copy)
4. Filter by cropland year thresholds using DuckDB on the Arrow table
5. Eliminate small polygons by merging into longest-shared-boundary neighbor
6. Simplify (Douglas-Peucker)
7. Write to GeoParquet
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import rioxarray  # noqa: F401
import xarray as xr

from csb.config import BARREN_CODE
from csb.io import write_geoparquet
from csb.vector import (
    arrow_to_geometries,
    eliminate_small_polygons,
    geometries_to_arrow,
    polygonize,
    simplify_geometries,
)

logger = logging.getLogger(__name__)


def _load_area_year(split_rasters: Path, area: str, year: int) -> np.ndarray:
    """Load and mosaic all raster tiles for one area + year into a single 2D array."""
    year_dir = split_rasters / str(year)
    tiles = sorted(year_dir.glob(f"{area}_{year}*.tif"))
    if not tiles:
        msg = f"No tiles for area={area} year={year} in {year_dir}"
        raise FileNotFoundError(msg)

    # Most areas have a single tile; just load it
    if len(tiles) == 1:
        da = xr.open_dataarray(tiles[0], engine="rasterio")
        arr = da.values
        meta = {
            "transform": da.rio.transform(),
            "crs": str(da.rio.crs),
        }
        da.close()
        return arr[0] if arr.ndim == 3 else arr, meta

    # Multiple tiles: mosaic
    arrays = [xr.open_dataarray(t, engine="rasterio") for t in tiles]
    merged = xr.concat(arrays, dim="band").squeeze("band", drop=True)
    arr = merged.values
    meta = {"transform": merged.rio.transform(), "crs": str(merged.rio.crs)}
    for a in arrays:
        a.close()
    return arr, meta


def _combine_years(
    split_rasters: Path, area: str, years: list[int]
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Stack rasters across years and compute COUNT0 and COUNT45.

    Returns:
        count0: 2D array — number of years each pixel has value > 0
        count45: 2D array — number of years each pixel equals BARREN_CODE (45)
        meta: dict with transform and crs from the first year loaded
    """
    stacks = []
    meta = None
    for year in years:
        arr, m = _load_area_year(split_rasters, area, year)
        stacks.append(arr)
        if meta is None:
            meta = m

    stack = np.stack(stacks, axis=0)  # (n_years, H, W)
    count0 = np.sum(stack > 0, axis=0).astype(np.int16)
    count45 = np.sum(stack == BARREN_CODE, axis=0).astype(np.int16)
    return count0, count45, meta


def process_area(args: tuple[str, dict[str, Any]]) -> str:
    """Process a single area tile through the full CREATE pipeline.

    Args:
        args: Tuple of (area_name, params_dict).

    Returns:
        Status string.
    """
    area, params = args
    cfg = params["config"]
    start_year: int = params["start_year"]
    end_year: int = params["end_year"]
    output_dir = Path(params["output_dir"])

    split_rasters = Path(cfg["paths"]["split_rasters"])
    min_cropland = cfg["global"]["min_cropland_years"]
    thresholds = cfg["create"]["eliminate_thresholds"]
    min_area = cfg["create"]["min_polygon_area"]
    simplify_tol = cfg["create"]["simplify_tolerance"]

    years = list(range(start_year, end_year + 1))
    t0 = time.perf_counter()

    # 1. Combine multi-year rasters
    logger.info(f"{area}: Combining {len(years)} years")
    count0, count45, meta = _combine_years(split_rasters, area, years)

    # 2. Compute effective count mask and polygonize
    effective = (count0 - count45).astype(np.int32)
    mask = effective >= 1
    if not mask.any():
        return f"Skipped {area} (no valid pixels)"

    logger.info(f"{area}: Polygonizing")
    table = polygonize(
        effective,
        mask=mask,
        transform=meta["transform"],
        nodata=0,
    )

    if table.num_rows == 0:
        return f"Skipped {area} (no polygons)"

    # 3. Filter by threshold using DuckDB directly on the Arrow table
    conn = duckdb.connect()
    conn.register("polys", table)
    filtered = conn.execute(f"""
        SELECT geometry, CAST(value AS INTEGER) AS effective_count
        FROM polys
        WHERE value >= {min_cropland}
    """).fetch_arrow_table()
    conn.close()

    if filtered.num_rows == 0:
        return f"Skipped {area} (all filtered)"

    # 4. Eliminate small polygons
    geoms = arrow_to_geometries(filtered)
    vals = filtered.column("effective_count").to_pylist()

    logger.info(f"{area}: Eliminating small polygons ({filtered.num_rows} input)")
    geoms, vals = eliminate_small_polygons(geoms, vals, thresholds)

    # 5. Filter by min area
    keep = [i for i, g in enumerate(geoms) if g.area >= min_area]
    geoms = [geoms[i] for i in keep]
    vals = [vals[i] for i in keep]

    if not geoms:
        return f"Skipped {area} (all below min area)"

    # 6. Simplify
    logger.info(f"{area}: Simplifying {len(geoms)} polygons")
    geoms = simplify_geometries(geoms, simplify_tol)

    # 7. Write GeoParquet
    out_table = geometries_to_arrow(
        geoms,
        columns={
            "effective_count": vals,
            "area_sqm": [g.area for g in geoms],
        },
    )
    out_path = output_dir / f"{area}_{start_year}_{end_year}.parquet"
    write_geoparquet(out_table, out_path)

    elapsed = (time.perf_counter() - t0) / 60
    logger.info(f"{area}: Done — {len(geoms)} polygons in {elapsed:.2f} min")
    return f"Finished {area} ({len(geoms)} polygons, {elapsed:.1f} min)"


def run_create(
    cfg: dict[str, Any],
    start_year: int,
    end_year: int,
    output_dir: str | Path,
    area: str | None = None,
) -> Path:
    """Run the CREATE stage for all (or one) area tile(s).

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

    from csb.parallel import parallel_map, worker_count

    console = Console()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    split_rasters = Path(cfg["paths"]["split_rasters"])
    year_dir = split_rasters / str(start_year)

    # Discover area tiles
    tif_files = sorted(year_dir.glob("*.tif"))
    areas = sorted({f.stem.rsplit(f"_{start_year}", 1)[0] for f in tif_files})
    if area:
        areas = [a for a in areas if a == area]

    # Skip already-done areas
    done = {f.stem.split(f"_{start_year}")[0] for f in output_dir.glob("*.parquet")}
    remaining = [a for a in areas if a not in done]

    n_workers = worker_count(cfg["global"]["cpu_fraction"])
    console.print(
        f"CREATE: {len(remaining)}/{len(areas)} areas, {start_year}-{end_year}, {n_workers} workers"
    )

    if not remaining:
        console.print("[green]All areas already processed.")
        return output_dir

    params = {
        "config": cfg,
        "start_year": start_year,
        "end_year": end_year,
        "output_dir": str(output_dir),
    }
    task_args = [(a, params) for a in remaining]
    results = parallel_map(process_area, task_args, max_workers=n_workers)

    for r in results:
        console.print(f"  {r}")

    console.print(f"[bold green]CREATE complete: {len(results)} areas")
    return output_dir
