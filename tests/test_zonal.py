"""Tests for csb.utils — zonal statistics."""

from __future__ import annotations

from typing import TYPE_CHECKING

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from shapely import box

from csb.utils import zonal_majority

if TYPE_CHECKING:
    from pathlib import Path


def _write_value_raster(
    path: Path, data: np.ndarray, transform: object, crs: str = "EPSG:5070"
) -> None:
    """Helper to write a small GeoTIFF."""
    profile = {
        "driver": "GTiff",
        "dtype": data.dtype,
        "width": data.shape[-1],
        "height": data.shape[-2],
        "count": 1,
        "crs": crs,
        "transform": transform,
        "nodata": 0,
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def test_zonal_majority_basic(tmp_path: Path):
    """Two zone polygons, each with a clear majority value."""
    transform = from_bounds(0, 0, 300, 300, 10, 10)

    # Value raster: top half = 61, bottom half = 176
    values = np.zeros((10, 10), dtype=np.int32)
    values[0:5, :] = 61
    values[5:10, :] = 176

    value_path = tmp_path / "values.tif"
    _write_value_raster(value_path, values, transform)

    # Zone polygons matching top/bottom halves
    zones = gpd.GeoDataFrame(
        {"zone_id": [1, 2]},
        geometry=[box(0, 150, 300, 300), box(0, 0, 300, 150)],
        crs="EPSG:5070",
    )

    result = zonal_majority(zones, "zone_id", value_path)

    assert isinstance(result, dict)
    assert result[1] == 61
    assert result[2] == 176


def test_zonal_majority_single_zone(tmp_path: Path):
    """Single zone covering the entire raster."""
    transform = from_bounds(0, 0, 150, 150, 5, 5)

    values = np.full((5, 5), 42, dtype=np.int32)
    value_path = tmp_path / "values.tif"
    _write_value_raster(value_path, values, transform)

    zones = gpd.GeoDataFrame(
        {"zone_id": [1]},
        geometry=[box(0, 0, 150, 150)],
        crs="EPSG:5070",
    )

    result = zonal_majority(zones, "zone_id", value_path)
    assert result[1] == 42


def test_zonal_majority_mixed_values(tmp_path: Path):
    """Zone with mixed values should return the most common one."""
    transform = from_bounds(0, 0, 150, 150, 5, 5)

    # 20 pixels = 61, 5 pixels = 176 → majority = 61
    values = np.full((5, 5), 61, dtype=np.int32)
    values[4, :] = 176

    value_path = tmp_path / "values.tif"
    _write_value_raster(value_path, values, transform)

    zones = gpd.GeoDataFrame(
        {"zone_id": [1]},
        geometry=[box(0, 0, 150, 150)],
        crs="EPSG:5070",
    )

    result = zonal_majority(zones, "zone_id", value_path)
    assert result[1] == 61
