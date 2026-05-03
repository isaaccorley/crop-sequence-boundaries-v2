#!/bin/bash
#SBATCH --job-name=csb-pmtiles
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=400G
#SBATCH --time=06:00:00
#SBATCH --output=data/logs/pmtiles_%j.out
#SBATCH --error=data/logs/pmtiles_%j.err

set -euo pipefail
mkdir -p data/logs
cd "$SLURM_SUBMIT_DIR"

# Toolchain — reuse the FTW pmtiles env from /projects (tippecanoe 2.79).
TIPPECANOE=/projects/bgtj/isaaccorley/envs/ftw-tile/bin/tippecanoe
"$TIPPECANOE" --version

INPUT_PARQUET=data/output/conus/postprocess/2018_2025/national/CSB1825.parquet
FINAL_PMTILES=data/output/conus/postprocess/2018_2025/CSB1825.pmtiles
TMP_FGB="$TMPDIR/csb1825.fgb"
TMP_PMTILES="$TMPDIR/csb1825.pmtiles"

echo "==== build_pmtiles starting at $(date) ===="
echo "Node: $SLURMD_NODENAME, cpus=$SLURM_CPUS_PER_TASK, TMPDIR=$TMPDIR"
df -h $TMPDIR

# 1) GeoParquet -> FlatGeobuf in $TMPDIR (NVMe, ~1.5 TB)
t0=$(date +%s)
uv run python scripts/parquet_to_pmtiles.py "$INPUT_PARQUET" "$TMP_FGB"
t1=$(date +%s)
echo "==== parquet -> fgb done in $((t1-t0))s, $(du -h $TMP_FGB | cut -f1) ===="

# 2) FGB -> PMTiles via tippecanoe.
# Config tuned for USDA-CSB-style polygons (30m source, CONUS extent, agriculture).
# - minzoom=4: continental view comfortably loads at z=4.
# - maxzoom=12: 30m source is ~38m/px at z=12 (1.27x oversample).
# - base-zoom=12: hold full detail until maxzoom.
# - simplification=1: source already simplified at 60m via shapely.coverage_simplify.
# - drop-densest-as-needed: REQUIRED (tippecanoe errors at byte cap without it).
# - coalesce-smallest-as-needed: merge sub-pixel adjacent same-class fields at low zooms.
# - 2 MB / 1M-feature tile caps: web renderers handle this, gives nice low-zoom views.
# - -y attrs: carry the USDA-schema columns most consumers care about.
"$TIPPECANOE" \
    -o "$TMP_PMTILES" \
    -l csb \
    --minimum-zoom=4 \
    --maximum-zoom=12 \
    --base-zoom=12 \
    --full-detail=12 \
    --simplification=1 \
    --buffer=8 \
    --maximum-tile-bytes=2000000 \
    --maximum-tile-features=1000000 \
    --drop-densest-as-needed \
    --coalesce-smallest-as-needed \
    --detect-shared-borders \
    --read-parallel \
    -y CSBID \
    -y CDL2018 -y CDL2019 -y CDL2020 -y CDL2021 \
    -y CDL2022 -y CDL2023 -y CDL2024 -y CDL2025 \
    --force \
    "$TMP_FGB"
t2=$(date +%s)
echo "==== tippecanoe done in $((t2-t1))s, $(du -h $TMP_PMTILES | cut -f1) ===="

# 3) Move pmtiles to /projects, drop the FGB intermediate.
cp "$TMP_PMTILES" "$FINAL_PMTILES"
rm -f "$TMP_FGB" "$TMP_PMTILES"
t3=$(date +%s)
echo "==== copy + cleanup done in $((t3-t2))s ===="

echo ""
echo "==== build complete ===="
echo "  pmtiles: $FINAL_PMTILES ($(du -h $FINAL_PMTILES | cut -f1))"
echo "  total wall: $((t3-t0))s"
echo "==== finished at $(date) ===="
