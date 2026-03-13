"""Tests for csb.distribute — _build_national, _compute_fields, _export_state."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely import box, to_wkb

from csb.distribute import _build_national, _compute_fields, _export_state, run_distribute
from csb.io import write_geoparquet

if TYPE_CHECKING:
    from pathlib import Path


def _make_prep_parquet(path, n=5, statefips="17", csbyears="2024"):
    """Create a small PREP-like parquet for testing."""
    geoms = [to_wkb(box(i * 100, 0, (i + 1) * 100, 100)) for i in range(n)]
    table = pa.table({
        "geometry": pa.array(geoms, type=pa.binary()),
        "effective_count": pa.array(list(range(1, n + 1)), type=pa.int32()),
        "STATEFIPS": pa.array([statefips] * n, type=pa.string()),
        "STATEASD": pa.array(["1710"] * n, type=pa.string()),
        "ASD": pa.array(["10"] * n, type=pa.string()),
        "CNTY": pa.array(["Cook"] * n, type=pa.string()),
        "CNTYFIPS": pa.array(["031"] * n, type=pa.string()),
        "CSBYEARS": pa.array([csbyears] * n, type=pa.string()),
        "CSBID": pa.array([""] * n, type=pa.string()),
    })
    write_geoparquet(table, path)
    return path


def test_build_national(tmp_path: Path):
    prep_dir = tmp_path / "prep"
    prep_dir.mkdir()
    _make_prep_parquet(prep_dir / "area1.parquet", n=3)
    _make_prep_parquet(prep_dir / "area2.parquet", n=4)

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    count = _build_national(conn, prep_dir)
    assert count == 7

    cols = [row[0] for row in conn.execute("DESCRIBE national").fetchall()]
    assert "national_oid" in cols
    conn.close()


def test_build_national_empty(tmp_path: Path):
    prep_dir = tmp_path / "prep"
    prep_dir.mkdir()

    conn = duckdb.connect()
    with pytest.raises(FileNotFoundError, match="No PREP parquets"):
        _build_national(conn, prep_dir)
    conn.close()


def test_compute_fields(tmp_path: Path):
    prep_dir = tmp_path / "prep"
    prep_dir.mkdir()
    _make_prep_parquet(prep_dir / "area1.parquet", n=3)

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    _build_national(conn, prep_dir)
    _compute_fields(conn)

    result = conn.execute("SELECT CSBACRES, INSIDE_X, INSIDE_Y, CSBID FROM national LIMIT 1").fetchone()
    assert result is not None
    csbacres, inside_x, inside_y, csbid = result
    assert csbacres > 0
    assert inside_x is not None
    assert inside_y is not None
    assert len(csbid) > 0
    conn.close()


def test_compute_fields_csbid_format(tmp_path: Path):
    """CSBID should be STATEFIPS + CSBYEARS + zero-padded national_oid."""
    prep_dir = tmp_path / "prep"
    prep_dir.mkdir()
    _make_prep_parquet(prep_dir / "area1.parquet", n=2, statefips="17", csbyears="2024")

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    _build_national(conn, prep_dir)
    _compute_fields(conn)

    rows = conn.execute("SELECT CSBID, STATEFIPS, CSBYEARS FROM national ORDER BY national_oid").fetchall()
    for csbid, fips, csbyears in rows:
        assert csbid.startswith(fips)
        assert csbyears in csbid
    conn.close()


def _make_national_parquet(path, n=3, statefips="17"):
    """Create a national-like parquet with national_oid and computed fields."""
    geoms = [to_wkb(box(i * 100, 0, (i + 1) * 100, 100)) for i in range(n)]
    table = pa.table({
        "geometry": pa.array(geoms, type=pa.binary()),
        "effective_count": pa.array(list(range(1, n + 1)), type=pa.int32()),
        "STATEFIPS": pa.array([statefips] * n, type=pa.string()),
        "CSBYEARS": pa.array(["2024"] * n, type=pa.string()),
        "CSBID": pa.array([f"{statefips}2024{str(i).zfill(9)}" for i in range(1, n + 1)], type=pa.string()),
        "CSBACRES": pa.array([10.0] * n, type=pa.float64()),
        "INSIDE_X": pa.array([50.0 + i * 100 for i in range(n)], type=pa.float64()),
        "INSIDE_Y": pa.array([50.0] * n, type=pa.float64()),
        "national_oid": pa.array(list(range(1, n + 1)), type=pa.int64()),
    })
    write_geoparquet(table, path)
    return path


def test_export_state(tmp_path: Path):
    """_export_state should produce a GeoParquet file for the state."""
    national_path = tmp_path / "national.parquet"
    _make_national_parquet(national_path, n=3, statefips="17")

    output_dir = tmp_path / "state"
    output_dir.mkdir(parents=True)

    params = {
        "national_parquet": str(national_path),
        "output_dir": str(output_dir),
        "csb_tag": "2024",
    }

    result = _export_state("IL", "17", params)
    assert "Finished" in result
    assert "3 features" in result

    parquet_out = output_dir / "CSBIL2024.parquet"
    assert parquet_out.exists()
    table = pq.read_table(parquet_out)
    assert table.num_rows == 3


def test_export_state_no_data(tmp_path: Path):
    """_export_state with non-matching FIPS should skip."""
    national_path = tmp_path / "national.parquet"
    _make_national_parquet(national_path, n=3, statefips="17")

    output_dir = tmp_path / "state"
    output_dir.mkdir(parents=True)

    params = {
        "national_parquet": str(national_path),
        "output_dir": str(output_dir),
        "csb_tag": "2024",
    }

    result = _export_state("CA", "06", params)
    assert "Skipped" in result


def test_run_distribute(tmp_path: Path):
    """Full run_distribute with small synthetic data."""
    prep_dir = tmp_path / "prep"
    prep_dir.mkdir()
    _make_prep_parquet(prep_dir / "area1.parquet", n=3, statefips="17", csbyears="2024")

    output_dir = tmp_path / "output"

    cfg = {
        "global": {"cpu_fraction": 0.5},
    }

    with (
        patch("csb.distribute.STATE_FIPS", {"IL": "17"}),
        patch(
            "csb.utils.parallel_starmap",
            side_effect=lambda fn, items, **kw: [fn(*args) for args in items],
        ),
    ):
        result = run_distribute(cfg, 2020, 2024, prep_dir, output_dir)

    assert result.exists()
    national = output_dir / "national" / "CSB2024.parquet"
    assert national.exists()
