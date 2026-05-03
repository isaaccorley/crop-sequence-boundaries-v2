#!/bin/bash
#SBATCH --job-name=csb-parity-conus
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --output=data/logs/conus_parity_%j.out
#SBATCH --error=data/logs/conus_parity_%j.err

set -euo pipefail
mkdir -p data/logs
cd "$SLURM_SUBMIT_DIR"

uv run python scripts/conus_parity.py \
    --national data/output/conus/postprocess/2018_2025/national/CSB1825_indexed.parquet \
    --gdb data/CSB1825_indexed.parquet \
    --report data/profile/conus_parity.json
