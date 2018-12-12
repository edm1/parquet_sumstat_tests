#!/usr/bin/env bash
#

set -euo pipefail

# Local
echo "Testing local"
python read_parquet_with_dask.py local/GTEX7
# GCS
echo "Testing gcs"
python read_parquet_with_dask.py gs://genetics-portal-sumstats/test/GTEX7

echo COMPLETE
