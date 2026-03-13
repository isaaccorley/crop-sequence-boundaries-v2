"""Tests for csb.config."""

from __future__ import annotations

from typing import TYPE_CHECKING

from csb.config import (
    ACRES_PER_SQM,
    BARREN_CODE,
    DEFAULT_CRS,
    STATE_FIPS,
    bundled_config_path,
    load_config,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_load_config(default_config_path: Path):
    cfg = load_config(default_config_path)
    assert cfg["global"]["min_cropland_years"] == 2


def test_config_sections(default_config: dict):
    for section in ("global", "paths", "create"):
        assert section in default_config


def test_bundled_config_exists():
    path = bundled_config_path()
    assert path.exists()


def test_state_fips():
    assert len(STATE_FIPS) == 48  # CONUS
    assert STATE_FIPS["AL"] == "01"
    assert STATE_FIPS["WY"] == "56"


def test_constants():
    import pytest

    assert BARREN_CODE == 45
    assert DEFAULT_CRS == "EPSG:5070"
    assert ACRES_PER_SQM == pytest.approx(1.0 / 4046.86)
