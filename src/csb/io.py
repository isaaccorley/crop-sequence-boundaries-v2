"""I/O utilities for GeoParquet."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


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
