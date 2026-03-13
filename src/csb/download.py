"""Download USDA Cropland Data Layer (CDL) rasters from NASS."""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

CDL_BASE_URL = "https://www.nass.usda.gov/Research_and_Science/Cropland/Release/datasets"

# Years that have 10m CDL available (in addition to 30m)
YEARS_WITH_10M = {2024, 2025}

# Earliest year with national CDL
MIN_YEAR = 2008


def cdl_url(year: int, resolution: int = 30) -> str:
    """Return the download URL for a CDL zip file.

    Args:
        year: CDL year (2008–present).
        resolution: Pixel size in meters (10 or 30).

    Returns:
        Full URL to the zip file.
    """
    if year < MIN_YEAR:
        msg = f"National CDL not available before {MIN_YEAR}, got {year}"
        raise ValueError(msg)
    if resolution == 10 and year not in YEARS_WITH_10M:
        msg = f"10m CDL only available for {sorted(YEARS_WITH_10M)}, got {year}"
        raise ValueError(msg)
    return f"{CDL_BASE_URL}/{year}_{resolution}m_cdls.zip"


def download_cdl(
    years: Sequence[int],
    output_dir: str | Path,
    resolution: int = 30,
    overwrite: bool = False,
) -> list[Path]:
    """Download and extract CDL rasters for the given years.

    Args:
        years: List of years to download.
        output_dir: Root directory. Files are saved to <output_dir>/<year>/.
        resolution: Pixel size in meters (10 or 30).
        overwrite: Re-download even if the file already exists.

    Returns:
        List of paths to extracted TIF files.
    """
    import urllib.request

    from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        TextColumn,
        TransferSpeedColumn,
    )

    output_dir = Path(output_dir)
    extracted: list[Path] = []

    for year in years:
        year_dir = output_dir / str(year)
        tif_name = f"{year}_{resolution}m_cdls.tif"
        tif_path = year_dir / tif_name

        if tif_path.exists() and not overwrite:
            logger.info(f"{year}: Already exists at {tif_path}")
            extracted.append(tif_path)
            continue

        year_dir.mkdir(parents=True, exist_ok=True)
        url = cdl_url(year, resolution)
        zip_path = year_dir / f"{year}_{resolution}m_cdls.zip"

        logger.info(f"{year}: Downloading {url}")
        with Progress(
            TextColumn(f"[bold blue]{year}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
        ) as progress:
            task = progress.add_task("download", total=None)

            def _reporthook(block_num: int, block_size: int, total_size: int) -> None:
                if total_size > 0:
                    progress.update(task, total=total_size)
                progress.update(task, advance=block_size)

            urllib.request.urlretrieve(url, zip_path, reporthook=_reporthook)  # noqa: S310

        logger.info(f"{year}: Extracting {zip_path}")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(year_dir)
        zip_path.unlink()

        if tif_path.exists():
            extracted.append(tif_path)
        else:
            # Find whatever .tif was extracted
            tifs = list(year_dir.glob("*.tif"))
            if tifs:
                extracted.append(tifs[0])
            else:
                logger.warning(f"{year}: No .tif found after extraction")

    return extracted
