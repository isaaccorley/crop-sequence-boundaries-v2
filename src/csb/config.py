"""Configuration loading and defaults."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# State abbreviation -> FIPS code (CONUS only)
STATE_FIPS: dict[str, str] = {
    "AL": "01",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "FL": "12",
    "GA": "13",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}

FIPS_TO_STATE: dict[str, str] = {v: k for k, v in STATE_FIPS.items()}

# Barren land CDL code (131 reclassed to 45)
BARREN_CODE = 45

# Albers Equal Area Conic (USGS version) — default CRS
DEFAULT_CRS = "EPSG:5070"

# Default raster cell size in meters
DEFAULT_CELL_SIZE = 30

# Conversion factor
ACRES_PER_SQM = 1.0 / 4046.86


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML config file and return as dict."""
    with Path(path).open() as f:
        return yaml.safe_load(f)


def bundled_config_path() -> Path:
    """Return path to the default config bundled with the package."""
    return Path(__file__).parent.parent.parent / "configs" / "default.yaml"
