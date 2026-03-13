"""Stage 2: PREP — Spatial join boundaries, compute zonal CDL stats per polygon.

Per area parquet (embarrassingly parallel):
1. Load area GeoParquet into DuckDB
2. Spatial join with county/ASD boundaries (largest overlap)
3. Zonal stats per year: majority CDL value (exactextract)
5. Bulk-join CDL columns via temp table, filter nulls
6. Write enriched GeoParquet
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from shapely import from_wkb

from csb.config import DEFAULT_CRS
from csb.io import write_geoparquet
from csb.utils import zonal_majority

logger = logging.getLogger(__name__)


def _spatial_join_boundaries(
    conn: duckdb.DuckDBPyConnection,
    boundaries_path: Path,
) -> None:
    """Spatial join area polygons with boundary features, keeping largest overlap."""
    suffix = boundaries_path.suffix.lower()
    if suffix == ".parquet":
        conn.execute(f"CREATE TABLE boundaries AS SELECT * FROM '{boundaries_path}'")
    else:
        conn.execute(f"CREATE TABLE boundaries AS SELECT * FROM ST_Read('{boundaries_path}')")

    conn.execute("""
        CREATE TABLE area_joined AS
        WITH ranked AS (
            SELECT
                a.*,
                b.STATEFIPS,
                b.STATEASD,
                b.ASD,
                b.CNTY,
                b.CNTYFIPS,
                ROW_NUMBER() OVER (
                    PARTITION BY a.row_id
                    ORDER BY ST_Area(ST_Intersection(
                        a.geometry,
                        b.geometry
                    )) DESC
                ) AS rn
            FROM area a
            JOIN boundaries b
            ON ST_Intersects(a.geometry, b.geometry)
        )
        SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
    """)
    conn.execute("DROP TABLE area; ALTER TABLE area_joined RENAME TO area")


def process_area(args: tuple[Path, dict[str, Any]]) -> str:
    """Process a single area parquet through the full PREP pipeline."""
    parquet_path, params = args
    cfg = params["config"]
    start_year: int = params["start_year"]
    end_year: int = params["end_year"]
    output_dir = Path(params["output_dir"])
    boundaries_path = Path(cfg["paths"]["boundaries"])
    national_cdl = Path(cfg["paths"]["national_cdl"])

    area_name = parquet_path.stem
    csb_years = f"{str(start_year)[2:]}{str(end_year)[2:]}"
    t0 = time.perf_counter()

    # DuckDB with spatial
    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    logger.info(f"{area_name}: Loading and joining boundaries")
    conn.execute(
        f"CREATE TABLE area AS SELECT *, ROW_NUMBER() OVER () AS row_id FROM '{parquet_path}'"
    )

    # Add CSB metadata columns
    conn.execute(f"""
        ALTER TABLE area ADD COLUMN CSBYEARS VARCHAR DEFAULT '{csb_years}';
        ALTER TABLE area ADD COLUMN CSBID VARCHAR;
    """)

    # Spatial join
    _spatial_join_boundaries(conn, boundaries_path)

    # Check we have data
    row = conn.execute("SELECT COUNT(*) FROM area").fetchone()
    assert row is not None
    count = row[0]
    if count == 0:
        conn.close()
        return f"Skipped {area_name} (empty after join)"

    # Rasterize for zone IDs
    logger.info(f"{area_name}: Rasterizing {count} polygons for zonal stats")
    result = conn.execute("SELECT row_id, geometry FROM area").fetchall()
    row_ids = [r[0] for r in result]
    geoms = [from_wkb(r[1]) for r in result]

    import geopandas as gpd

    gdf = gpd.GeoDataFrame({"zone_id": row_ids}, geometry=geoms, crs=DEFAULT_CRS)

    # Zonal stats per year — bulk UPDATE via temp table instead of row-by-row
    for year in range(start_year, end_year + 1):
        logger.info(f"{area_name}: Zonal stats {year}")
        cdl_path = national_cdl / str(year) / f"{year}_30m_cdls.tif"
        zone_to_cdl = zonal_majority(gdf, "zone_id", cdl_path)

        col = f"CDL{year}"
        conn.execute(f"ALTER TABLE area ADD COLUMN {col} INTEGER")

        if zone_to_cdl:
            # Register as Arrow table for bulk UPDATE
            zonal_table = pa.table(
                {
                    "zone_id": pa.array(list(zone_to_cdl.keys()), type=pa.int64()),
                    "cdl_val": pa.array(list(zone_to_cdl.values()), type=pa.int32()),
                }
            )
            conn.register("zonal_tmp", zonal_table)
            conn.execute(f"""
                UPDATE area SET {col} = z.cdl_val
                FROM zonal_tmp z
                WHERE area.row_id = z.zone_id
            """)
            conn.unregister("zonal_tmp")

    # Filter nulls (polygon had no CDL coverage)
    conn.execute(f"DELETE FROM area WHERE CDL{end_year} IS NULL")

    # Export
    logger.info(f"{area_name}: Exporting enriched parquet")
    out_table = conn.execute("SELECT * EXCLUDE (row_id) FROM area").arrow().read_all()
    out_path = output_dir / f"{area_name}.parquet"
    write_geoparquet(out_table, out_path)
    conn.close()

    elapsed = (time.perf_counter() - t0) / 60
    logger.info(f"{area_name}: Done in {elapsed:.2f} min")
    return f"Finished {area_name} ({out_table.num_rows} features, {elapsed:.1f} min)"


def run_prep(
    cfg: dict[str, Any],
    start_year: int,
    end_year: int,
    create_dir: str | Path,
    output_dir: str | Path,
) -> Path:
    """Run the PREP stage on all CREATE output parquets.

    Args:
        cfg: Loaded config dict.
        start_year: First CDL year.
        end_year: Last CDL year (inclusive).
        create_dir: Directory containing CREATE stage output parquets.
        output_dir: Directory for PREP output parquets.

    Returns:
        Path to the output directory.
    """
    from rich.console import Console

    from csb.utils import parallel_map, worker_count

    console = Console()
    create_dir = Path(create_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(create_dir.glob("*.parquet"))
    console.print(f"PREP: Found {len(parquet_files)} area tiles from CREATE")

    # Skip done
    done = {f.stem for f in output_dir.glob("*.parquet")}
    remaining = [f for f in parquet_files if f.stem not in done]

    if not remaining:
        console.print("[green]All areas already prepped.")
        return output_dir

    n_workers = worker_count(cfg["global"]["cpu_fraction"])
    console.print(f"PREP: {len(remaining)} areas, {n_workers} workers")

    params = {
        "config": cfg,
        "start_year": start_year,
        "end_year": end_year,
        "output_dir": str(output_dir),
    }
    task_args = [(f, params) for f in remaining]
    results = parallel_map(process_area, task_args, max_workers=n_workers)

    for r in results:
        console.print(f"  {r}")

    console.print(f"[bold blue]PREP complete: {len(results)} areas")
    return output_dir
