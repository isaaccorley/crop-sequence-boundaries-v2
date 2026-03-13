"""Tests for csb.prep — _spatial_join_boundaries and run_prep."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import duckdb
import pyarrow as pa
from shapely import box, to_wkb

from csb.io import write_geoparquet
from csb.prep import _spatial_join_boundaries, run_prep

if TYPE_CHECKING:
    from pathlib import Path


def _make_area_table(conn: duckdb.DuckDBPyConnection, tmp_path: Path, n: int = 3) -> None:
    """Create a small area table in DuckDB with row_id and geometry (via GeoParquet)."""
    geoms = [to_wkb(box(i * 100, 0, (i + 1) * 100, 100)) for i in range(n)]
    table = pa.table(
        {
            "geometry": pa.array(geoms, type=pa.binary()),
            "effective_count": pa.array(list(range(1, n + 1)), type=pa.int32()),
        }
    )
    area_path = tmp_path / "_area_tmp.parquet"
    write_geoparquet(table, area_path)
    conn.execute(
        f"CREATE TABLE area AS SELECT *, ROW_NUMBER() OVER () AS row_id FROM '{area_path}'"
    )


def _make_boundaries_parquet(path: Path, n: int = 1) -> Path:
    """Create a boundary parquet that covers the test area."""
    # One big boundary covering everything
    geom = to_wkb(box(-100, -100, 1000, 1000))
    table = pa.table(
        {
            "geometry": pa.array([geom], type=pa.binary()),
            "STATEFIPS": pa.array(["17"], type=pa.string()),
            "STATEASD": pa.array(["1710"], type=pa.string()),
            "ASD": pa.array(["10"], type=pa.string()),
            "CNTY": pa.array(["Cook"], type=pa.string()),
            "CNTYFIPS": pa.array(["031"], type=pa.string()),
        }
    )
    write_geoparquet(table, path)
    return path


def test_spatial_join_boundaries(tmp_path: Path) -> None:
    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    _make_area_table(conn, tmp_path, n=3)
    boundary_path = _make_boundaries_parquet(tmp_path / "boundaries.parquet")

    _spatial_join_boundaries(conn, boundary_path)

    # Should still have 3 rows with boundary columns added
    count = conn.execute("SELECT COUNT(*) FROM area").fetchone()
    assert count is not None
    assert count[0] == 3

    # Check boundary columns were joined
    cols = [row[0] for row in conn.execute("DESCRIBE area").fetchall()]
    assert "STATEFIPS" in cols
    assert "CNTYFIPS" in cols
    conn.close()


def test_spatial_join_no_overlap(tmp_path: Path) -> None:
    """Boundaries that don't overlap any area polygons → empty result."""
    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    _make_area_table(conn, tmp_path, n=2)

    # Boundary far away from area polygons
    geom = to_wkb(box(10000, 10000, 20000, 20000))
    table = pa.table(
        {
            "geometry": pa.array([geom], type=pa.binary()),
            "STATEFIPS": pa.array(["06"], type=pa.string()),
            "STATEASD": pa.array(["0610"], type=pa.string()),
            "ASD": pa.array(["10"], type=pa.string()),
            "CNTY": pa.array(["LA"], type=pa.string()),
            "CNTYFIPS": pa.array(["037"], type=pa.string()),
        }
    )
    boundary_path = tmp_path / "boundaries_far.parquet"
    write_geoparquet(table, boundary_path)

    _spatial_join_boundaries(conn, boundary_path)

    count = conn.execute("SELECT COUNT(*) FROM area").fetchone()
    assert count is not None
    assert count[0] == 0
    conn.close()


def test_run_prep_skips_done(tmp_path: Path) -> None:
    """Already-processed areas should be skipped."""
    create_dir = tmp_path / "create"
    create_dir.mkdir()
    output_dir = tmp_path / "prep"
    output_dir.mkdir()

    # Create a fake input parquet
    (create_dir / "T1.parquet").touch()

    # Mark as done
    (output_dir / "T1.parquet").touch()

    cfg = {
        "global": {"cpu_fraction": 0.5},
        "paths": {"boundaries": "/fake", "national_cdl": "/fake"},
    }

    result = run_prep(cfg, 2020, 2022, create_dir, output_dir)
    assert result == output_dir


def test_run_prep_no_files(tmp_path: Path) -> None:
    """No parquets in create_dir → empty run."""
    create_dir = tmp_path / "create"
    create_dir.mkdir()
    output_dir = tmp_path / "prep"

    cfg = {
        "global": {"cpu_fraction": 0.5},
        "paths": {"boundaries": "/fake", "national_cdl": "/fake"},
    }

    with patch("csb.utils.parallel_map", return_value=[]):
        result = run_prep(cfg, 2020, 2022, create_dir, output_dir)

    assert result.exists()
