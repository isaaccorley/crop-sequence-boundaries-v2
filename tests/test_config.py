"""Tests for csb.config."""

from __future__ import annotations

from typing import TYPE_CHECKING

from csb.config import (
    ACRES_PER_SQM,
    BARREN_CODE,
    DEFAULT_CELL_SIZE,
    DEFAULT_CRS,
    FIPS_TO_STATE,
    STATE_FIPS,
    bundled_config_path,
    load_config,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_load_config(default_config_path: Path):
    cfg = load_config(default_config_path)
    assert cfg["global"]["min_cropland_years"] == 2
    assert "states" in cfg
    assert cfg["states"]["AL"] == "01"


def test_config_sections(default_config: dict):
    for section in ("global", "paths", "create", "raster", "states"):
        assert section in default_config


def test_bundled_config_exists():
    path = bundled_config_path()
    assert path.exists()


def test_state_fips_complete():
    assert len(STATE_FIPS) == 48  # CONUS


def test_fips_to_state_inverse():
    for abbr, fips in STATE_FIPS.items():
        assert FIPS_TO_STATE[fips] == abbr


def test_constants():
    import pytest

    assert BARREN_CODE == 45
    assert DEFAULT_CRS == "EPSG:5070"
    assert DEFAULT_CELL_SIZE == 30
    assert ACRES_PER_SQM == pytest.approx(1.0 / 4046.86)
