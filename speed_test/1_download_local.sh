#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p local

gsutil -m cp -r gs://genetics-portal-sumstats/test/GTEX7 local

# Alt method
# cd local
# ln -s /Users/em21/Projects/genetics-finemapping/input/molecular_qtl/GTEX7

echo COMPLETE
