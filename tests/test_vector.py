"""Tests for csb.vector."""

from __future__ import annotations

import numpy as np
import pyarrow as pa
from rasterio.transform import from_bounds
from shapely import Point, box, to_wkb

from csb.vector import (
    arrow_to_geometries,
    eliminate_small_polygons,
    geometries_to_arrow,
    polygonize,
    simplify_geometries,
)


def test_polygonize_basic(sample_raster: np.ndarray, sample_transform):
    table = polygonize(sample_raster, transform=sample_transform, nodata=0)
    assert isinstance(table, pa.Table)
    assert "geometry" in table.column_names
    assert "value" in table.column_names
    # 4 zones should produce 4 polygons
    assert table.num_rows == 4


def test_polygonize_with_mask(sample_raster: np.ndarray, sample_transform):
    mask = np.ones_like(sample_raster, dtype=np.bool_)
    mask[5:, :] = False  # mask out bottom half
    table = polygonize(sample_raster, mask=mask, transform=sample_transform, nodata=0)
    # Only top-half zones (1, 2)
    assert table.num_rows == 2


def test_polygonize_empty():
    data = np.zeros((5, 5), dtype=np.int32)
    transform = from_bounds(0, 0, 150, 150, 5, 5)
    table = polygonize(data, transform=transform, nodata=0)
    assert table.num_rows == 0


def test_eliminate_small_polygons():
    # Create a large polygon and a tiny one touching it
    big = box(0, 0, 100, 100)
    small = box(100, 0, 101, 1)  # area = 1 sq unit
    medium = box(0, 100, 100, 200)

    geoms = [big, small, medium]
    vals = [1, 2, 3]

    result_geoms, result_vals = eliminate_small_polygons(geoms, vals, thresholds=[5])
    # small should be merged into big (they share a boundary)
    assert len(result_geoms) == 2
    assert 2 not in result_vals


def test_eliminate_no_small_polygons():
    big1 = box(0, 0, 100, 100)
    big2 = box(100, 0, 200, 100)

    geoms, _vals = eliminate_small_polygons([big1, big2], [1, 2], thresholds=[5])
    assert len(geoms) == 2


def test_eliminate_multiple_thresholds():
    big = box(0, 0, 200, 200)
    med = box(200, 0, 210, 10)  # area = 100
    small = box(0, 200, 2, 201)  # area = 2

    geoms = [big, med, small]
    vals = [1, 2, 3]

    result_geoms, _result_vals = eliminate_small_polygons(geoms, vals, thresholds=[5, 200])
    # small (area=2) merged at threshold 5, med (area=100) merged at threshold 200
    assert len(result_geoms) == 1


def test_simplify_geometries():
    # Create a polygon with many vertices
    from shapely.geometry import Polygon

    coords = [
        (0, 0),
        (1, 0.1),
        (2, 0),
        (3, 0.1),
        (4, 0),
        (4, 4),
        (3, 3.9),
        (2, 4),
        (1, 3.9),
        (0, 4),
        (0, 0),
    ]
    poly = Polygon(coords)

    simplified = simplify_geometries([poly], tolerance=0.5)
    assert len(simplified) == 1
    # Should have fewer vertices
    assert len(simplified[0].exterior.coords) < len(poly.exterior.coords)
    assert simplified[0].is_valid


def test_geometries_to_arrow():
    geoms = [Point(0, 0), Point(1, 1)]
    table = geometries_to_arrow(geoms, columns={"id": [1, 2]})

    assert table.num_rows == 2
    assert "geometry" in table.column_names
    assert "id" in table.column_names


def test_arrow_to_geometries():
    geoms = [Point(0, 0), Point(1, 1)]
    wkbs = [to_wkb(g) for g in geoms]
    table = pa.table({"geometry": pa.array(wkbs, type=pa.binary())})

    result = arrow_to_geometries(table)
    assert len(result) == 2
    assert result[0].equals(Point(0, 0))
    assert result[1].equals(Point(1, 1))


def test_geometries_arrow_roundtrip():
    original = [box(0, 0, 10, 10), box(10, 0, 20, 10)]
    table = geometries_to_arrow(original, columns={"val": [100, 200]})
    recovered = arrow_to_geometries(table)

    for orig, rec in zip(original, recovered, strict=True):
        assert orig.equals(rec)
