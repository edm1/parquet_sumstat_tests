#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p input_data

wget https://www.dropbox.com/s/nwvc2u9n8mqzlki/20015_raw.gwas.imputed_v3.both_sexes.tsv.bgz?dl=0 -O input_data/20015_raw.gwas.imputed_v3.both_sexes.tsv.bgz

echo COMPLETE
