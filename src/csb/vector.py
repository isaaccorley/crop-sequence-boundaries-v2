"""Vector geometry operations — polygonize, eliminate, simplify."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np  # noqa: TC002
import pyarrow as pa
from contourrs import shapes_arrow
from shapely import from_wkb, make_valid, to_wkb
from shapely.ops import unary_union
from shapely.strtree import STRtree

logger = logging.getLogger(__name__)


def polygonize(
    data: np.ndarray,
    mask: np.ndarray | None = None,
    transform: Any = None,
    connectivity: int = 4,
    nodata: int | None = 0,
) -> pa.Table:
    """Convert a raster to polygon geometries as an Arrow table.

    Uses contourrs.shapes_arrow for zero-copy Rust→Arrow polygonization.

    Args:
        data: 2D integer array of raster values.
        mask: Optional boolean mask (True = valid pixels).
        transform: Affine transform for georeferencing.
        connectivity: Pixel connectivity (4 or 8).
        nodata: Value to exclude from polygonization.

    Returns:
        PyArrow Table with 'geometry' (WKB) and 'value' (float64) columns.
    """
    return shapes_arrow(
        data,
        mask=mask,
        connectivity=connectivity,
        transform=transform,
        nodata=nodata,
    )


def eliminate_small_polygons(
    geometries: list[Any],
    values: list[Any],
    thresholds: list[float],
) -> tuple[list[Any], list[Any]]:
    """Iteratively merge small polygons into the neighbor with the longest shared boundary.

    Mirrors arcpy.management.Eliminate(selection="LENGTH"). For each threshold
    (ascending), polygons with area <= threshold are dissolved into their
    neighbor that shares the longest boundary segment.

    Args:
        geometries: List of shapely geometries.
        values: Parallel list of values per polygon.
        thresholds: Area thresholds in ascending order (sq meters).

    Returns:
        (filtered_geometries, filtered_values) after elimination.
    """
    geoms = list(geometries)
    vals = list(values)

    for threshold in thresholds:
        tree = STRtree(geoms)
        merge_map: dict[int, int] = {}

        for i, geom in enumerate(geoms):
            if geom.area > threshold:
                continue

            candidates = tree.query(geom, predicate="touches")
            best_idx = -1
            best_length = 0.0

            for j in candidates:
                if j == i or geoms[j].area <= threshold:
                    continue
                shared = geom.boundary.intersection(geoms[j].boundary)
                if shared.length > best_length:
                    best_length = shared.length
                    best_idx = j

            if best_idx >= 0:
                merge_map[i] = best_idx

        merged: set[int] = set()
        for small_idx, big_idx in merge_map.items():
            geoms[big_idx] = make_valid(unary_union([geoms[big_idx], geoms[small_idx]]))
            merged.add(small_idx)

        geoms = [g for i, g in enumerate(geoms) if i not in merged]
        vals = [v for i, v in enumerate(vals) if i not in merged]

    return geoms, vals


def simplify_geometries(
    geometries: list[Any],
    tolerance: float,
) -> list[Any]:
    """Simplify geometries using Douglas-Peucker with topology preservation.

    Args:
        geometries: List of shapely geometries.
        tolerance: Simplification tolerance in CRS units (meters for EPSG:5070).

    Returns:
        List of simplified, valid geometries.
    """
    result = []
    for geom in geometries:
        simplified = geom.simplify(tolerance, preserve_topology=True)
        if not simplified.is_valid:
            simplified = make_valid(simplified)
        result.append(simplified)
    return result


def geometries_to_arrow(
    geometries: list[Any],
    columns: dict[str, list[Any]] | None = None,
) -> pa.Table:
    """Convert shapely geometries + optional columns to an Arrow table with WKB geometry."""
    wkb_geoms = [to_wkb(g) for g in geometries]
    data: dict[str, Any] = {"geometry": pa.array(wkb_geoms, type=pa.binary())}
    if columns:
        data.update(columns)
    return pa.table(data)


def arrow_to_geometries(table: pa.Table, column: str = "geometry") -> list[Any]:
    """Extract shapely geometries from a WKB column in an Arrow table."""
    return [from_wkb(g.as_py()) for g in table.column(column)]
