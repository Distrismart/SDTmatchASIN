#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source .venv/bin/activate
set -a; source .env; set +a
mkdir -p output logs
python3 quick_match.py \
  --input data/eans.csv \
  --output output/matches.csv \
  --marketplaces de,fr \
  | tee "logs/run_$(date +%F_%H%M).log"
