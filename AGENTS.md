# AGENTS.md

## Project Overview

**CSB** (Crop Sequence Boundaries) is a cloud-native geospatial pipeline that transforms USDA Cropland Data Layer (CDL) rasters into structured crop sequence boundary datasets at national, state, and county/ASD levels.

**Stack:** Python 3.12+, uv, contourrs (Rust polygonize), DuckDB spatial, exactextract, GeoParquet.

## Architecture

```text
CLI (cli.py)
├── download        → Download USDA CDL rasters
├── build-boundaries→ Build ASD+county boundary GeoParquet
├── create          → Windowed-read CDL → polygonize → eliminate → simplify (DuckDB)
├── prep            → Spatial join + zonal CDL stats per area
├── distribute      → Merge national → split by state → GeoParquet
└── run-all         → Execute full pipeline: create → prep → distribute
```

### Pipeline Data Flow

```text
CDL Rasters ──→ create ──→ prep ──→ distribute ──→ National GeoParquet
                             ↑                  ──→ State GeoParquet
                ASD/County ──┘
```

### Module Map

| Module          | Role                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------- |
| `cli.py`        | Click CLI; 7 commands                                                                       |
| `create.py`     | Windowed-read national CDL; polygonize; eliminate; simplify + filter in DuckDB              |
| `prep.py`       | Spatial join with boundaries; bulk zonal CDL stats via exactextract                         |
| `distribute.py` | Merge to national; split by state; export GeoParquet                                        |
| `download.py`   | Download USDA CDL rasters (30m/10m)                                                         |
| `boundaries.py` | Build ASD+county boundary file from TIGER/NASS                                              |
| `utils.py`      | Vector ops (polygonize, eliminate), zonal stats (exactextract), parallelism helpers         |
| `io.py`         | GeoParquet write                                                                            |
| `config.py`     | YAML config loader; STATE_FIPS mapping; constants (BARREN_CODE, DEFAULT_CRS, ACRES_PER_SQM) |

## Key Conventions

- **Config:** YAML-based (`configs/default.yaml`), override via `--config` flag.
- **CRS:** EPSG:5070 (Albers Equal Area Conic) throughout; hardcoded as `DEFAULT_CRS` constant.
- **Format:** GeoParquet throughout.
- **Parallelism:** ProcessPoolExecutor using 90% of CPUs. Each stage processes independent area tiles.
- **Resumable:** Each stage skips already-completed areas by checking output directory.
- **Vectorized ops:** Simplify, min-area filter, and derived field computation done in DuckDB SQL. Zonal stats bulk-updated via Arrow temp tables.

## Build & Test

```bash
make install    # uv sync --all-extras
make check      # pre-commit run --all-files (ruff, ty, pyproject-fmt, mdformat)
make test       # pytest --cov=src tests/
make build      # uv build
make clean      # remove build artifacts
```

## Coding Guidelines

- Python 3.12+ features are fine (type unions `X | Y`, etc.).
- Lint/format: ruff (line-length 100). Type check: ty.
- Keep files under ~500 LOC; split if larger.
- Tests in `tests/`; use pytest with xdist.
- Commits: Conventional Commits (`feat|fix|refactor|...`).
- Pre-commit hooks: ruff lint+format, ty check, pyproject-fmt, mdformat, uv lock.

## Dependencies

| Package      | Purpose                                |
| ------------ | -------------------------------------- |
| contourrs    | Rust raster→polygon (zero-copy Arrow)  |
| duckdb       | SQL spatial joins, filtering, simplify |
| exactextract | C++ zonal statistics                   |
| shapely      | Geometry operations (eliminate)        |
| polars       | DataFrame operations                   |
| rasterio     | GeoTIFF windowed reads                 |
| click        | CLI framework                          |
| rich         | Terminal progress bars                 |
