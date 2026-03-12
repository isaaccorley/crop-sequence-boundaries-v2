"""Parallelism helpers."""

from __future__ import annotations

import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


def worker_count(cpu_fraction: float = 0.90) -> int:
    """Return number of worker processes based on CPU fraction."""
    total = multiprocessing.cpu_count()
    return max(1, round(cpu_fraction * total))


def parallel_map(
    fn: Callable[..., Any],
    items: list[Any],
    max_workers: int | None = None,
) -> list[Any]:
    """Map fn over items using ProcessPoolExecutor. Returns results in order."""
    max_workers = max_workers or worker_count()
    logger.info(f"Running {len(items)} tasks across {max_workers} workers")
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(fn, items))


def parallel_starmap(
    fn: Callable[..., Any],
    items: list[tuple[Any, ...]],
    max_workers: int | None = None,
) -> list[Any]:
    """Like parallel_map but unpacks tuple args via starmap."""
    max_workers = max_workers or worker_count()
    logger.info(f"Running {len(items)} tasks across {max_workers} workers")
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(fn, *args) for args in items]
        return [f.result() for f in futures]
