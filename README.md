# CSB — Crop Sequence Boundaries

Cloud-native geospatial pipeline for generating USDA Crop Sequence Boundaries from Cropland Data Layer (CDL) rasters.

CSB converts yearly national CDL rasters into tiled crop-sequence polygons, enriches them with county and ASD context plus zonal CDL attributes, then publishes national and state GeoParquet outputs.

## Architecture

[View interactive diagram on Excalidraw](https://excalidraw.com/#json=a_prbpp_P7ZiXLGUepdqU,n96ghtNeEzddZlpic4vu2g)

```text
                          INPUT
  ┌──────────┐                         ┌────────────┐
  │ download │────────────────────▸    │ CDL Rasters│
  └──────────┘                         └─────┬──────┘
  ┌──────────────────┐                       │
  │ build-boundaries │──────▸ ┌──────────┐   │
  └──────────────────┘        │ ASD/CNTY │   │
                              └────┬─────┘   │
                          PIPELINE │         │
            ┌────────┐     ┌──────┴┐   ┌────┴──────┐
            │ create │────▸│ prep  │──▸│distribute │
            └────────┘     └───────┘   └─────┬─────┘
            Polygonize      Join +       Merge +
            + eliminate     zonal stats  split by state
                                              │
                          OUTPUT              │
  ┌───────────────────┐  ┌────────────────┐
  │National GeoParquet│  │State GeoParquet│
  └───────────────────┘  └────────────────┘
```

## Pipeline

```text
download         Download USDA national CDL rasters (30 m, and 10 m for 2024+)
build-boundaries Build CONUS county + ASD boundary GeoParquet
create           Windowed-read CDL -> combine yearly sequences -> polygonize -> eliminate -> simplify
prep             Spatial join boundaries -> zonal majority CDL per year
distribute       Merge area outputs -> derive IDs/metrics -> write national + state GeoParquet
run-all          Execute create -> prep -> distribute
```

Key implementation details:

- CRS is fixed to `EPSG:5070` throughout.
- Outputs are GeoParquet at every stage.
- Parallel stages use `ProcessPoolExecutor` with a configurable CPU fraction.
- Stages are resumable: completed area tiles are skipped automatically.

## Install

For development or local runs, install from source:

```bash
make install
```

That runs `uv sync --all-extras` and installs the CLI entrypoint as `csb`.

If you only want a local package install without the dev toolchain:

```bash
pip install .
```

## Configuration

CSB uses YAML configuration. The bundled default is [configs/default.yaml](/home/isaaccorley/github/crop-sequence-boundaries-v2/configs/default.yaml).

Default paths:

```yaml
paths:
  output: /data/csb/output
  national_cdl: /data/csb/input/national_cdl
  boundaries: /data/csb/input/boundaries/US48_ASD_CNTY_Albers.parquet
```

Run any command with a custom config:

```bash
csb --config configs/local.yaml run-all 2020 2024
```

## Usage

Build the two required inputs first:

```bash
csb download 2020 2024
csb build-boundaries
```

Run the full pipeline:

```bash
csb run-all 2020 2024
```

Or run stages individually:

```bash
csb create 2020 2024 --output /data/csb/output/create/2020_2024
csb prep 2020 2024 --create-dir /data/csb/output/create/2020_2024 --output /data/csb/output/prep/2020_2024
csb distribute 2020 2024 --prep-dir /data/csb/output/prep/2020_2024 --output /data/csb/output/distribute/2020_2024
```

Useful options:

- `csb download ... --resolution 10` for 10 m CDL where available (`2024`, `2025`).
- `csb download ... --overwrite` to force a fresh download.
- `csb create ... --area A0` to process a single tile during debugging.
- `csb --config path/to/config.yaml ...` to override storage paths or thresholds.

## Outputs

For a run over `2020 2024`, the default output tree is:

```text
/data/csb/output/
├── create/2020_2024/*.parquet
├── prep/2020_2024/*.parquet
└── distribute/2020_2024/
    ├── national/CSB2024.parquet
    └── state/CSB<STATE>2024.parquet
```

The DISTRIBUTE stage adds derived fields including:

- `CSBID` — stable identifier built from state FIPS, year tag, and national row ID.
- `CSBACRES` — polygon area in acres.
- `INSIDE_X`, `INSIDE_Y` — point-on-surface coordinates in `EPSG:5070`.

The PREP stage enriches each polygon with:

- `STATEFIPS`, `STATEASD`, `ASD`, `CNTY`, `CNTYFIPS`
- `CDL<year>` majority-value columns for each requested year

## Development

```bash
make install
make check
make test
make build
```

Targets:

- `make install` — `uv sync --all-extras`
- `make check` — `uv run pre-commit run --all-files`
- `make test` — `uv run pytest --cov=src tests/`
- `make build` — `uv build`

## Stack

- `contourrs` for Rust-backed raster polygonization
- `duckdb` with spatial extension for filtering, joins, and derived geometry fields
- `exactextract` for zonal statistics
- `rasterio` for windowed GeoTIFF reads
- `shapely` for polygon elimination workflows
- `polars` and `pyarrow` for columnar data handling
- `GeoParquet` as the storage format throughout
