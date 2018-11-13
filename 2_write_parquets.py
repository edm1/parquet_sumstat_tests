#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import os
import pandas as pd

def main():

    # Args
    in_data = 'input_data/20015_raw.gwas.imputed_v3.both_sexes.tsv.bgz'
    out_path = 'parquet_files'
    row_offset = 100000 # Number of rows per row group

    # Make out path
    os.makedirs(out_path, exist_ok=True)

    # Read data
    print('Reading...')
    data = pd.read_csv(in_data, sep='\t', compression='gzip', header=0) #, nrows=1000000)
    data['chrom'], data['pos'], data['ref'], data['alt'] = data.variant.str.split(':').str
    data['chrom'] = data['chrom'].astype(str)
    data['pos'] = data['pos'].astype(int)

    # Shuffle data
    print('Shuffling data...')
    data = data.sample(frac=1).reset_index(drop=True)

    # Sort by (chrom, pos) and write without partitioning
    print('Method 1...')
    outf = os.path.join(out_path, 'chrom-pos-sort_no-partitioning')
    data.sort_values(['chrom', 'pos']) \
        .to_parquet(outf,
                    engine='fastparquet',
                    compression='snappy',
                    file_scheme='hive',
                    row_group_offsets=row_offset
            )

    # Sort by (chrom) and write without partitioning
    print('Method 2...')
    outf = os.path.join(out_path, 'chrom-sort_no-partitioning')
    data.sort_values(['chrom']) \
        .to_parquet(outf,
                    engine='fastparquet',
                    compression='snappy',
                    file_scheme='hive',
                    row_group_offsets=row_offset
            )

    # Sort by (pos) and write without partitioning
    print('Method 3...')
    outf = os.path.join(out_path, 'pos-sort_no-partitioning')
    data.sort_values(['pos']) \
        .to_parquet(outf,
                    engine='fastparquet',
                    compression='snappy',
                    file_scheme='hive',
                    row_group_offsets=row_offset
            )

    # Sort by (pos) and partition on chrom
    print('Method 4...')
    outf = os.path.join(out_path, 'pos-sort_chrom-partitioning')
    data.sort_values('pos') \
        .to_parquet(outf,
                    engine='fastparquet',
                    compression='snappy',
                    file_scheme='hive',
                    partition_on=['chrom'],
                    row_group_offsets=row_offset
            )



    return 0

if __name__ == '__main__':

    main()
