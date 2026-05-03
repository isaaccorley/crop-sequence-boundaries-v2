"""GeoParquet -> FlatGeobuf converter for the CSB pmtiles build.

Reads the CONUS national CSB GeoParquet, drops invalid/empty/null geometries
(pyogrio's FGB writer with spatial index errors out on NULLs), reprojects to
EPSG:4326 (tippecanoe expects lon/lat input), and writes a single FlatGeobuf
to $OUT.

Usage:
    uv run python scripts/parquet_to_pmtiles.py <input.parquet> <output.fgb>
"""

import sys
import time
from pathlib import Path

import geopandas as gpd
import pyogrio
import shapely


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: parquet_to_pmtiles.py <input.parquet> <output.fgb>")
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])

    print(f"reading {inp} ({inp.stat().st_size / 1e9:.2f} GB)")
    t0 = time.perf_counter()
    gdf = gpd.read_parquet(inp)
    print(f"  loaded {len(gdf):,} features in {time.perf_counter() - t0:.1f}s")

    # Drop NULLs / empty / invalid (mandatory before FGB write per pmtiles-pipeline skill).
    t0 = time.perf_counter()
    keep = gdf.geometry.notna() & ~gdf.geometry.is_empty
    valid_mask = shapely.is_valid(gdf.geometry.values)
    keep &= valid_mask
    n_dropped = int((~keep).sum())
    if n_dropped:
        print(f"  dropped {n_dropped:,} invalid/empty/null geoms")
        gdf = gdf.loc[keep].reset_index(drop=True)
    print(f"  validity check {time.perf_counter() - t0:.1f}s")

    # Tippecanoe ingests lon/lat (EPSG:4326). Source is EPSG:5070 Albers.
    t0 = time.perf_counter()
    if str(gdf.crs).split(":")[-1] != "4326":
        print(f"  reprojecting {gdf.crs} -> EPSG:4326")
        gdf = gdf.to_crs("EPSG:4326")
    print(f"  reproject {time.perf_counter() - t0:.1f}s")

    # CSBID, CSBYEARS, STATEFIPS, STATEASD, ASD, CNTY, CNTYFIPS are short
    # strings — leave as-is. CDL{year} is int32, CSBACRES is float64.
    # Drop national_oid (internal), Shape_area / Shape_Length (derivable),
    # area_sqm (duplicate of CSBACRES).
    drop_cols = {"national_oid", "Shape_area", "Shape_Length", "area_sqm"}
    cols = [c for c in gdf.columns if c not in drop_cols]
    gdf = gdf[cols]
    print(f"  carrying columns: {[c for c in cols if c != 'geometry']}")

    print(f"writing FlatGeobuf -> {out}")
    t0 = time.perf_counter()
    pyogrio.write_dataframe(gdf, out, driver="FlatGeobuf")
    print(f"  wrote {out.stat().st_size / 1e9:.2f} GB in {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
