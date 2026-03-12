"""Tests for csb.io."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from shapely import Point, to_wkb

from csb.io import (
    load_raster_numpy,
    read_geoparquet,
    write_cog,
    write_geoparquet,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_load_raster_numpy(sample_raster_path: Path, sample_raster: np.ndarray):
    data, meta = load_raster_numpy(sample_raster_path)
    assert data.shape == (10, 10)
    np.testing.assert_array_equal(data, sample_raster)
    assert meta["crs"] == "EPSG:5070"
    assert meta["height"] == 10
    assert meta["width"] == 10


def test_write_cog(tmp_path: Path):
    data = np.arange(100, dtype=np.int32).reshape(10, 10)
    transform = rasterio.transform.from_bounds(0, 0, 300, 300, 10, 10)
    path = write_cog(data, tmp_path / "test.tif", transform=transform, crs="EPSG:5070")

    assert path.exists()
    with rasterio.open(path) as src:
        assert src.profile["driver"] == "GTiff"
        assert src.profile["compress"] == "deflate"
        assert src.crs.to_epsg() == 5070
        assert len(src.overviews(1)) > 0
        read_data = src.read(1)
        np.testing.assert_array_equal(read_data, data)


def test_write_cog_no_overviews(tmp_path: Path):
    data = np.ones((5, 5), dtype=np.uint8)
    transform = rasterio.transform.from_bounds(0, 0, 150, 150, 5, 5)
    path = write_cog(data, tmp_path / "no_ov.tif", transform=transform, overviews=False)

    with rasterio.open(path) as src:
        assert src.overviews(1) == []


def test_write_cog_3d_input(tmp_path: Path):
    data = np.ones((1, 5, 5), dtype=np.uint8)
    transform = rasterio.transform.from_bounds(0, 0, 150, 150, 5, 5)
    path = write_cog(data, tmp_path / "3d.tif", transform=transform, overviews=False)
    assert path.exists()


def test_write_geoparquet(tmp_path: Path):
    geom = to_wkb(Point(100, 200))
    table = pa.table(
        {
            "geometry": pa.array([geom], type=pa.binary()),
            "value": pa.array([42], type=pa.int32()),
        }
    )
    path = write_geoparquet(table, tmp_path / "test.parquet")
    assert path.exists()

    # Verify geo metadata
    meta = pq.read_schema(path).metadata
    geo = json.loads(meta[b"geo"])
    assert geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"]["encoding"] == "WKB"


def test_read_geoparquet(tmp_path: Path):
    geom = to_wkb(Point(100, 200))
    table = pa.table(
        {
            "geometry": pa.array([geom], type=pa.binary()),
            "name": pa.array(["test"], type=pa.string()),
        }
    )
    path = write_geoparquet(table, tmp_path / "roundtrip.parquet")

    result = read_geoparquet(path)
    assert result.num_rows == 1
    assert result.column("name")[0].as_py() == "test"
