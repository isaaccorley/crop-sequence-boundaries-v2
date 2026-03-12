"""I/O utilities for rasters, GeoParquet, and COGs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np  # noqa: TC002
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import xarray as xr
from rasterio.enums import Resampling

from csb.config import DEFAULT_CRS


def load_raster(path: str | Path, chunks: dict[str, int] | None = None) -> xr.DataArray:
    """Load a GeoTIFF as an xarray DataArray with optional dask chunking."""
    import rioxarray  # noqa: F401

    return xr.open_dataarray(path, engine="rasterio", chunks=chunks)


def load_raster_numpy(path: str | Path) -> tuple[np.ndarray, dict[str, Any]]:
    """Load a GeoTIFF into a numpy array + metadata dict.

    Returns (data, meta) where meta has keys: transform, crs, height, width, nodata.
    """
    with rasterio.open(path) as src:
        data = src.read(1)
        meta = {
            "transform": src.transform,
            "crs": str(src.crs),
            "height": src.height,
            "width": src.width,
            "nodata": src.nodata,
        }
    return data, meta


def write_cog(
    data: np.ndarray,
    path: str | Path,
    transform: Any,
    crs: str = DEFAULT_CRS,
    nodata: int = 0,
    overviews: bool = True,
) -> Path:
    """Write a numpy array as a Cloud Optimized GeoTIFF.

    Args:
        data: 2D or 3D array (band, height, width).
        path: Output file path.
        transform: Affine transform.
        crs: Coordinate reference system string.
        nodata: Nodata value.
        overviews: Whether to build pyramid overviews.

    Returns:
        Path to the written file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if data.ndim == 3:
        data = data[0]

    profile = {
        "driver": "GTiff",
        "dtype": data.dtype,
        "width": data.shape[1],
        "height": data.shape[0],
        "count": 1,
        "crs": crs,
        "transform": transform,
        "nodata": nodata,
        "compress": "deflate",
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
    }

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)

    if overviews:
        with rasterio.open(path, "r+") as dst:
            factors = [2, 4, 8, 16]
            dst.build_overviews(factors, Resampling.nearest)
            dst.update_tags(ns="rio_overview", resampling="nearest")

    return path


def write_geoparquet(
    table: pa.Table,
    path: str | Path,
    geometry_column: str = "geometry",
    crs_code: int = 5070,
) -> Path:
    """Write a PyArrow table as GeoParquet with proper geo metadata.

    Args:
        table: Arrow table with a WKB geometry column.
        path: Output file path.
        geometry_column: Name of the geometry column.
        crs_code: EPSG code for the CRS.

    Returns:
        Path to the written file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    metadata = dict(table.schema.metadata or {})
    geo_meta = {
        "version": "1.1.0",
        "primary_column": geometry_column,
        "columns": {
            geometry_column: {
                "encoding": "WKB",
                "geometry_types": [],
                "crs": {"id": {"authority": "EPSG", "code": crs_code}},
            }
        },
    }
    metadata[b"geo"] = json.dumps(geo_meta).encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path)
    return path


def read_geoparquet(path: str | Path) -> pa.Table:
    """Read a GeoParquet file as an Arrow table."""
    return pq.read_table(path)
