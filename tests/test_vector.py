"""Tests for csb.utils — vector operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pytest
from rasterio.transform import from_bounds
from shapely import box, to_wkb

if TYPE_CHECKING:
    from affine import Affine
    from sedonadb.context import SedonaContext

from csb.utils import (
    eliminate_small_polygons,
    make_sedona,
    polygonize,
)


@pytest.fixture
def sd() -> SedonaContext:
    return make_sedona()


def _make_table(geoms: list, vals: list) -> pa.Table:
    """Build an Arrow table with WKB geometry + effective_count from Shapely geoms."""
    wkbs = [to_wkb(g) for g in geoms]
    return pa.table(
        {
            "geometry": pa.array(wkbs, type=pa.binary()),
            "effective_count": pa.array(vals, type=pa.int32()),
        }
    )


def test_polygonize_basic(sample_raster: np.ndarray, sample_transform: Affine) -> None:
    table = polygonize(sample_raster, transform=sample_transform, nodata=0)
    assert isinstance(table, pa.Table)
    assert "geometry" in table.column_names
    assert "value" in table.column_names
    # 4 zones should produce 4 polygons
    assert table.num_rows == 4


def test_polygonize_with_mask(sample_raster: np.ndarray, sample_transform: Affine) -> None:
    mask = np.ones_like(sample_raster, dtype=np.bool_)
    mask[5:, :] = False  # mask out bottom half
    table = polygonize(sample_raster, mask=mask, transform=sample_transform, nodata=0)
    # Only top-half zones (1, 2)
    assert table.num_rows == 2


def test_polygonize_empty() -> None:
    data = np.zeros((5, 5), dtype=np.int32)
    transform = from_bounds(0, 0, 150, 150, 5, 5)
    table = polygonize(data, transform=transform, nodata=0)
    assert table.num_rows == 0


def test_eliminate_small_polygons(sd: SedonaContext) -> None:
    # large polygon and a tiny one touching it
    big = box(0, 0, 100, 100)
    small = box(100, 0, 101, 1)  # area = 1 sq unit
    medium = box(0, 100, 100, 200)

    table = _make_table([big, small, medium], [1, 2, 3])
    result = eliminate_small_polygons(table, thresholds=[5], sd=sd)

    # small should be merged into big (they share a boundary)
    assert result.num_rows == 2
    assert 2 not in result.column("effective_count").to_pylist()


def test_eliminate_no_small_polygons(sd: SedonaContext) -> None:
    big1 = box(0, 0, 100, 100)
    big2 = box(100, 0, 200, 100)

    table = _make_table([big1, big2], [1, 2])
    result = eliminate_small_polygons(table, thresholds=[5], sd=sd)
    assert result.num_rows == 2


def test_eliminate_multiple_thresholds(sd: SedonaContext) -> None:
    big = box(0, 0, 200, 200)
    med = box(200, 0, 210, 10)  # area = 100
    small = box(0, 200, 2, 201)  # area = 2

    table = _make_table([big, med, small], [1, 2, 3])
    result = eliminate_small_polygons(table, thresholds=[5, 200], sd=sd)
    # small (area=2) merged at threshold 5, med (area=100) merged at threshold 200
    assert result.num_rows == 1


def test_eliminate_empty_table(sd: SedonaContext) -> None:
    table = pa.table(
        {
            "geometry": pa.array([], type=pa.binary()),
            "effective_count": pa.array([], type=pa.int32()),
        }
    )
    result = eliminate_small_polygons(table, thresholds=[5], sd=sd)
    assert result.num_rows == 0
