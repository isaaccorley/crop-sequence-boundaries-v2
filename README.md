# CSB — Crop Sequence Boundaries

Cloud-native pipeline for generating USDA Crop Sequence Boundaries from Cropland Data Layer (CDL) rasters.

## Install

```bash
pip install csb
```

## Usage

```bash
# Full pipeline for 2020-2024
csb run-all 2020 2024

# Or run stages individually
csb create 2020 2024 -o /data/csb/create
csb prep 2020 2024 --create-dir /data/csb/create -o /data/csb/prep
csb distribute 2020 2024 --prep-dir /data/csb/prep -o /data/csb/distribute
```

## Stack

- **contourrs** — Rust-backed raster-to-polygon (zero-copy Arrow output)
- **rusterize** — Rust-backed polygon-to-raster
- **DuckDB spatial** — SQL vector processing, joins, filters
- **exactextract** — C++ zonal statistics
- **rioxarray** — Raster I/O with dask support
- **GeoParquet** — Cloud-native vector format throughout
- **COG** — Cloud Optimized GeoTIFF output
