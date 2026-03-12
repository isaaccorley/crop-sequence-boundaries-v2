"""CLI entrypoint for the CSB pipeline."""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console

from csb.config import bundled_config_path, load_config

console = Console()


@click.group()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default=None,
    help="Path to YAML config. Defaults to bundled configs/default.yaml.",
)
@click.pass_context
def main(ctx: click.Context, config: str | None) -> None:
    """CSB — Crop Sequence Boundaries pipeline.

    Generate national crop sequence boundary datasets from USDA CDL rasters
    for any user-specified time range.

    Stages: create -> prep -> distribute (or run-all for the full pipeline).
    """
    ctx.ensure_object(dict)
    cfg_path = config or bundled_config_path()
    ctx.obj["config"] = load_config(cfg_path)


@main.command()
@click.argument("start_year", type=int)
@click.argument("end_year", type=int)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output directory. Defaults to <config.paths.output>/create/<years>/.",
)
@click.option("--area", "-a", default=None, help="Process a single area tile.")
@click.pass_context
def create(
    ctx: click.Context, start_year: int, end_year: int, output: str | None, area: str | None
) -> None:
    """Stage 1: Combine CDL rasters -> polygonize -> eliminate -> simplify."""
    from csb.create import run_create

    cfg = ctx.obj["config"]
    out = (
        Path(output)
        if output
        else Path(cfg["paths"]["output"]) / "create" / f"{start_year}_{end_year}"
    )
    run_create(cfg, start_year, end_year, out, area=area)


@main.command()
@click.argument("start_year", type=int)
@click.argument("end_year", type=int)
@click.option(
    "--create-dir",
    type=click.Path(exists=True),
    required=True,
    help="Path to CREATE output directory.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output directory. Defaults to <config.paths.output>/prep/<years>/.",
)
@click.pass_context
def prep(
    ctx: click.Context, start_year: int, end_year: int, create_dir: str, output: str | None
) -> None:
    """Stage 2: Spatial join + zonal CDL stats."""
    from csb.prep import run_prep

    cfg = ctx.obj["config"]
    out = (
        Path(output)
        if output
        else Path(cfg["paths"]["output"]) / "prep" / f"{start_year}_{end_year}"
    )
    run_prep(cfg, start_year, end_year, create_dir, out)


@main.command()
@click.argument("start_year", type=int)
@click.argument("end_year", type=int)
@click.option(
    "--prep-dir", type=click.Path(exists=True), required=True, help="Path to PREP output directory."
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output directory. Defaults to <config.paths.output>/distribute/<years>/.",
)
@click.pass_context
def distribute(
    ctx: click.Context, start_year: int, end_year: int, prep_dir: str, output: str | None
) -> None:
    """Stage 3: Merge national -> split by state -> export COGs + GeoParquet."""
    from csb.distribute import run_distribute

    cfg = ctx.obj["config"]
    out = (
        Path(output)
        if output
        else Path(cfg["paths"]["output"]) / "distribute" / f"{start_year}_{end_year}"
    )
    run_distribute(cfg, start_year, end_year, prep_dir, out)


@main.command(name="run-all")
@click.argument("start_year", type=int)
@click.argument("end_year", type=int)
@click.option("--output", "-o", type=click.Path(), default=None, help="Root output directory.")
@click.pass_context
def run_all(ctx: click.Context, start_year: int, end_year: int, output: str | None) -> None:
    """Run the full pipeline: create -> prep -> distribute."""
    from csb.create import run_create
    from csb.distribute import run_distribute
    from csb.prep import run_prep

    cfg = ctx.obj["config"]
    base = Path(output) if output else Path(cfg["paths"]["output"])
    tag = f"{start_year}_{end_year}"

    console.print(f"[bold]Running full CSB pipeline for {start_year}-{end_year}")

    create_dir = run_create(cfg, start_year, end_year, base / "create" / tag)
    prep_dir = run_prep(cfg, start_year, end_year, create_dir, base / "prep" / tag)
    run_distribute(cfg, start_year, end_year, prep_dir, base / "distribute" / tag)

    console.print(f"[bold green]Pipeline complete. Output: {base}")
