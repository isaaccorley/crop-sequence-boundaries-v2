"""I/O utilities for GeoParquet."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS


def write_geoparquet(
    table: pa.Table,
    path: str | Path,
    geometry_column: str = "geometry",
    crs_code: int = 5070,
) -> Path:
    """Write a PyArrow table as GeoParquet with proper geo metadata.

    The CRS is serialized as full PROJJSON via `pyproj.CRS.to_json_dict()`.
    The short form `{"id": {"authority": "EPSG", "code": ...}}` lacks the
    required `"type"` field per PROJJSON spec; pyproj 3.x rejects it with
    "Internal Proj Error: proj_create: Missing 'type' key", which breaks
    downstream consumers (geopandas, pyogrio, tippecanoe via gdal).

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

    crs_projjson = CRS.from_epsg(crs_code).to_json_dict()
    metadata = dict(table.schema.metadata or {})
    geo_meta = {
        "version": "1.1.0",
        "primary_column": geometry_column,
        "columns": {
            geometry_column: {
                "encoding": "WKB",
                "geometry_types": [],
                "crs": crs_projjson,
            }
        },
    }
    metadata[b"geo"] = json.dumps(geo_meta).encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path)
    return path
