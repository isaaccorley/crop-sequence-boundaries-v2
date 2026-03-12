"""Zonal statistics using exactextract."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import numpy as np  # noqa: TC002
import rasterio
from exactextract import exact_extract


def zonal_majority(
    zone_raster: np.ndarray,
    zone_transform: Any,
    zone_crs: str,
    value_raster_path: str | Path,
) -> dict[int, int]:
    """Compute the majority value from a raster within each zone.

    Args:
        zone_raster: 2D integer array where each unique value is a zone ID.
        zone_transform: Affine transform for the zone raster.
        zone_crs: CRS string for the zone raster.
        value_raster_path: Path to the raster whose values are summarized.

    Returns:
        Dict mapping zone_id -> majority value.
    """
    # exact_extract needs file paths, so write zones to a temp raster
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp_path = tmp.name

    profile = {
        "driver": "GTiff",
        "dtype": zone_raster.dtype,
        "width": zone_raster.shape[-1],
        "height": zone_raster.shape[-2],
        "count": 1,
        "crs": zone_crs,
        "transform": zone_transform,
        "nodata": 0,
    }

    with rasterio.open(tmp_path, "w", **profile) as dst:
        if zone_raster.ndim == 2:
            dst.write(zone_raster, 1)
        else:
            dst.write(zone_raster[0], 1)

    try:
        results = exact_extract(
            str(value_raster_path),
            tmp_path,
            ["majority"],
            include_cols=["value"],
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    zone_to_majority: dict[int, int] = {}
    for row in results:
        zone_id = int(row["value"])
        majority = row.get("majority")
        if majority is not None:
            zone_to_majority[zone_id] = int(majority)

    return zone_to_majority
