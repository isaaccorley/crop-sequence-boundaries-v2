"""Stage 3: DISTRIBUTE — Merge national dataset, split by state, export GeoParquet.

1. Load all PREP parquets into DuckDB
2. Compute CSBACRES, INSIDE_X, INSIDE_Y
3. Generate CSBID = STATEFIPS + CSBYEARS + zero-padded national row ID
4. Write national GeoParquet
5. Per state (parallel): filter, write state GeoParquet
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import duckdb

from csb.config import ACRES_PER_SQM, STATE_FIPS
from csb.io import write_geoparquet

logger = logging.getLogger(__name__)


def _build_national(conn: duckdb.DuckDBPyConnection, prep_dir: Path) -> int:
    """Union all PREP parquets into a single national table. Returns row count."""
    parquets = sorted(prep_dir.glob("*.parquet"))
    if not parquets:
        msg = f"No PREP parquets in {prep_dir}"
        raise FileNotFoundError(msg)

    parts = [f"SELECT * FROM '{f}'" for f in parquets]
    conn.execute(f"""
        CREATE TABLE national AS
        SELECT *, ROW_NUMBER() OVER () AS national_oid
        FROM ({" UNION ALL ".join(parts)})
    """)
    row = conn.execute("SELECT COUNT(*) FROM national").fetchone()
    assert row is not None
    return row[0]


def _compute_fields(conn: duckdb.DuckDBPyConnection) -> None:
    """Add derived fields: CSBACRES, INSIDE_X, INSIDE_Y, final CSBID."""
    conn.execute(f"""
        ALTER TABLE national ADD COLUMN IF NOT EXISTS CSBACRES DOUBLE;
        UPDATE national SET CSBACRES = ST_Area(geometry) * {ACRES_PER_SQM};
    """)
    conn.execute("""
        ALTER TABLE national ADD COLUMN IF NOT EXISTS INSIDE_X DOUBLE;
        ALTER TABLE national ADD COLUMN IF NOT EXISTS INSIDE_Y DOUBLE;
        UPDATE national SET
            INSIDE_X = ST_X(ST_PointOnSurface(geometry)),
            INSIDE_Y = ST_Y(ST_PointOnSurface(geometry));
    """)
    conn.execute("""
        UPDATE national
        SET CSBID = STATEFIPS || CSBYEARS || LPAD(CAST(national_oid AS VARCHAR), 9, '0')
    """)


def _export_state(state: str, fips: str, params: dict[str, Any]) -> str:
    """Export a single state to GeoParquet."""
    national_parquet = Path(params["national_parquet"])
    output_dir = Path(params["output_dir"])
    csb_tag = params["csb_tag"]

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    state_table = (
        conn.execute(
            f"SELECT * EXCLUDE (national_oid) FROM '{national_parquet}' WHERE STATEFIPS = '{fips}'"
        )
        .arrow()
        .read_all()
    )
    conn.close()

    if state_table.num_rows == 0:
        return f"Skipped {state} (no data)"

    parquet_path = output_dir / f"CSB{state}{csb_tag}.parquet"
    write_geoparquet(state_table, parquet_path)

    logger.info(f"{state}: {state_table.num_rows} features exported")
    return f"Finished {state} ({state_table.num_rows} features)"


def run_distribute(
    cfg: dict[str, Any],
    start_year: int,
    end_year: int,
    prep_dir: str | Path,
    output_dir: str | Path,
) -> Path:
    """Run the DISTRIBUTE stage.

    Args:
        cfg: Loaded config dict.
        start_year: First CDL year.
        end_year: Last CDL year (inclusive).
        prep_dir: Directory containing PREP stage output parquets.
        output_dir: Root output directory for national + state outputs.

    Returns:
        Path to the output directory.
    """
    from rich.console import Console

    from csb.utils import parallel_starmap, worker_count

    console = Console()
    prep_dir = Path(prep_dir)
    output_dir = Path(output_dir)

    for sub in ("national", "state"):
        (output_dir / sub).mkdir(parents=True, exist_ok=True)

    csb_tag = f"{str(start_year)[2:]}{str(end_year)[2:]}"

    # 1. Build national table
    t0 = time.perf_counter()
    console.print("Merging subregions into national dataset...")

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    count = _build_national(conn, prep_dir)
    console.print(f"National: {count} features merged in {(time.perf_counter() - t0) / 60:.2f} min")

    # 2. Compute derived fields
    console.print("Computing CSBID, CSBACRES, INSIDE_X/Y...")
    _compute_fields(conn)

    # 3. Export national GeoParquet
    national_parquet = output_dir / "national" / f"CSB{csb_tag}.parquet"
    national_table = conn.execute("SELECT * FROM national").arrow().read_all()
    write_geoparquet(national_table, national_parquet)
    conn.close()
    console.print(f"National parquet: {national_parquet}")

    # 4. State exports (parallel)
    n_workers = worker_count(cfg["global"]["cpu_fraction"])
    console.print(f"Distributing to {len(STATE_FIPS)} states with {n_workers} workers...")

    params = {
        "national_parquet": str(national_parquet),
        "output_dir": str(output_dir / "state"),
        "csb_tag": csb_tag,
    }

    task_args = [(state, fips, params) for state, fips in STATE_FIPS.items()]
    results = parallel_starmap(_export_state, task_args, max_workers=n_workers)

    for r in results:
        console.print(f"  {r}")

    total = (time.perf_counter() - t0) / 60
    console.print(f"[bold magenta]DISTRIBUTE complete in {total:.2f} min")
    return output_dir
