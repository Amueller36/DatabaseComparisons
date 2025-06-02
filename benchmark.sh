#!/usr/bin/env bash
# Exit immediately if any command fails
set -e

# Jump to the project directory
cd ~/MDS/benchmarker || { echo "❌  ~/MDS/benchmarker not found"; exit 1; }

# Activate the virtual environment
source venv312/bin/activate

# Run the benchmarker
python benchmark.py "$@"

