"""Visual diff montage of the 3 lowest-IoU tiles (GA peanut, TX cotton, CA Central Valley).

One panel per tile: shared = grey, ours-only = blue, USDA-only = red.
"""
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import shapely
from csb.parity import find_bbox_5070
from matplotlib.collections import PolyCollection

ROOT = Path(__file__).resolve().parents[2]
OURS = ROOT / "data" / "output" / "conus" / "postprocess" / "2018_2025" / "national" / "CSB1825_indexed.parquet"
USDA = ROOT / "data" / "CSB1825_indexed.parquet"
OUT = Path(__file__).resolve().parent / "bottom3_montage.pdf"

# (region, tx, ty, label, IoU)
TILES = [
    ("GA peanut/cotton", 950_000, 1_100_000, "Georgia peanut", 0.543),
    ("Central TX cotton", -350_000, 900_000, "Texas cotton",  0.690),
    ("Central Valley CA", -2_000_000, 1_650_000, "Central Valley CA", 0.771),
]
def _query(parquet: Path, bx0, by0, bx1, by1):
    conn = duckdb.connect()
    conn.install_extension("spatial"); conn.load_extension("spatial")
    rows = conn.execute(f"""
        SELECT ST_AsWKB(ST_MakeValid(geometry))
        FROM read_parquet('{parquet}')
        WHERE xmax >= {bx0} AND xmin <= {bx1}
          AND ymax >= {by0} AND ymin <= {by1}
          AND ST_Intersects(geometry, ST_MakeEnvelope({bx0}, {by0}, {bx1}, {by1}))
    """).fetchall()
    conn.close()
    geoms = shapely.from_wkb([bytes(r[0]) for r in rows])
    return [g for g in geoms if g is not None and not g.is_empty]


def _flatten_paths(geoms):
    parts = shapely.get_parts(geoms)
    parts = parts[shapely.get_type_id(parts) == 3]
    return [np.asarray(p.exterior.coords) for p in parts if not p.is_empty]


def _draw_collection(ax, paths, fc):
    if not paths:
        return
    coll = PolyCollection(paths, facecolor=fc, edgecolor="none", linewidth=0,
                          rasterized=True)
    ax.add_collection(coll)


fig, axes = plt.subplots(1, 3, figsize=(7.0, 2.5))

for ax, (full_name, tx, ty, label, iou) in zip(axes, TILES):
    bx0, by0, bx1, by1 = find_bbox_5070(tx, ty)
    print(f"loading {label} bbox=({bx0:.0f},{by0:.0f})..({bx1:.0f},{by1:.0f})")
    ours = _query(OURS, bx0, by0, bx1, by1)
    usda = _query(USDA, bx0, by0, bx1, by1)
    print(f"  {label}: ours={len(ours)} usda={len(usda)}")

    empty = shapely.from_wkt("POLYGON EMPTY")
    try:
        ours_u = shapely.coverage_union_all(ours) if ours else empty
        usda_u = shapely.coverage_union_all(usda) if usda else empty
    except shapely.errors.GEOSException:
        ours_u = shapely.unary_union(ours) if ours else empty
        usda_u = shapely.unary_union(usda) if usda else empty
    ours_u = shapely.make_valid(ours_u)
    usda_u = shapely.make_valid(usda_u)

    inter = ours_u.intersection(usda_u)
    only_ours = ours_u.difference(usda_u)
    only_usda = usda_u.difference(ours_u)

    _draw_collection(ax, _flatten_paths([inter]), "#9a9a9a")
    _draw_collection(ax, _flatten_paths([only_ours]), "#2c5f8d")
    _draw_collection(ax, _flatten_paths([only_usda]), "#c0392b")

    ax.set_xlim(bx0, bx1)
    ax.set_ylim(by0, by1)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(f"{label}  (IoU {iou:.2f})", fontsize=9)
    for spine in ax.spines.values():
        spine.set_edgecolor("0.7"); spine.set_linewidth(0.5)

# Shared legend
legend_handles = [
    plt.Rectangle((0, 0), 1, 1, fc="#9a9a9a", ec="none"),
    plt.Rectangle((0, 0), 1, 1, fc="#2c5f8d", ec="none"),
    plt.Rectangle((0, 0), 1, 1, fc="#c0392b", ec="none"),
]
fig.legend(legend_handles, ["shared", "ours only", "USDA only"],
           loc="lower center", bbox_to_anchor=(0.5, -0.04),
           ncol=3, fontsize=8, frameon=False, handlelength=1.2)

fig.tight_layout()
fig.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
