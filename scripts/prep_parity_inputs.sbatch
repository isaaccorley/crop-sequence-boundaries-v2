#!/bin/bash
#SBATCH --job-name=csb-prep-parity
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --time=01:00:00
#SBATCH --output=data/logs/prep_parity_%j.out
#SBATCH --error=data/logs/prep_parity_%j.err

set -euo pipefail
mkdir -p data/logs
cd "$SLURM_SUBMIT_DIR"

uv run python scripts/prep_parity_inputs.py
