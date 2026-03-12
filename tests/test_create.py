"""Tests for csb.create stage."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from csb.config import BARREN_CODE
from csb.create import _combine_years, _load_area_year

if TYPE_CHECKING:
    from pathlib import Path


def test_load_area_year(multi_year_rasters: tuple[Path, list[int]]):
    base_dir, years = multi_year_rasters
    arr, meta = _load_area_year(base_dir, "G1", years[0])
    assert arr.shape == (20, 20)
    assert meta["crs"] == "EPSG:5070"
    assert meta["transform"] is not None


def test_combine_years(multi_year_rasters: tuple[Path, list[int]]):
    base_dir, years = multi_year_rasters
    count0, count45, _meta = _combine_years(base_dir, "G1", years)

    assert count0.shape == (20, 20)
    assert count45.shape == (20, 20)
    assert count0.dtype == np.int16
    assert count45.dtype == np.int16

    # count0 should be between 0 and len(years)
    assert count0.min() >= 0
    assert count0.max() <= len(years)

    # count45 should count barren pixels
    assert count45.min() >= 0
    assert count45.max() <= len(years)


def test_combine_years_counts_correctly(tmp_path: Path):
    """Verify count logic with deterministic data."""
    import rasterio
    from rasterio.transform import from_bounds

    transform = from_bounds(0, 0, 150, 150, 5, 5)
    profile = {
        "driver": "GTiff",
        "dtype": "int32",
        "width": 5,
        "height": 5,
        "count": 1,
        "crs": "EPSG:5070",
        "transform": transform,
        "nodata": 0,
    }

    # Year 1: all value=1 (cropland)
    year1_dir = tmp_path / "2020"
    year1_dir.mkdir()
    with rasterio.open(year1_dir / "A1_2020_0.tif", "w", **profile) as dst:
        dst.write(np.ones((5, 5), dtype=np.int32), 1)

    # Year 2: all value=45 (barren)
    year2_dir = tmp_path / "2021"
    year2_dir.mkdir()
    with rasterio.open(year2_dir / "A1_2021_0.tif", "w", **profile) as dst:
        dst.write(np.full((5, 5), BARREN_CODE, dtype=np.int32), 1)

    # Year 3: all zeros (no data)
    year3_dir = tmp_path / "2022"
    year3_dir.mkdir()
    with rasterio.open(year3_dir / "A1_2022_0.tif", "w", **profile) as dst:
        dst.write(np.zeros((5, 5), dtype=np.int32), 1)

    count0, count45, _meta = _combine_years(tmp_path, "A1", [2020, 2021, 2022])

    # Year 1 has value>0, year 2 has value>0 (45>0), year 3 has 0
    np.testing.assert_array_equal(count0, np.full((5, 5), 2))
    # Only year 2 has barren
    np.testing.assert_array_equal(count45, np.full((5, 5), 1))
