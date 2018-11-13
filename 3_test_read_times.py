#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import os
import pandas as pd
from glob import glob
import timeit
from functools import partial
import warnings

# Ignore the fastparquet warning
warnings.filterwarnings("ignore")
'''
/Users/em21/miniconda3/envs/finemapping/lib/python3.5/site-packages/fastparquet/api.py:212: UserWarning: Partition names coerce to values of different types, e.g. [1, 'X']
  warnings.warn("Partition names coerce to values of different types, e.g. %s" % examples)
'''

def main():

    # Args
    in_dir = 'parquet_files'
    region = {'chrom':'6', 'start':28477797, 'end':28478797}
    n_rep = 10 # Number of repititions for timer

    # Test each dataset, querying with (chrom, start end)
    print('\n#\n# Querying with (chrom, start end)\n#\n')
    for dataset in glob(os.path.join(in_dir, '*')):

        name = os.path.split(dataset)[1]
        print('# {0}'.format(name))

        # Load once to see how many rows are loaded with given filter
        res = read_filter(dataset, region['chrom'], region['start'], region['end'])
        print('Number of rows read: {0}'.format(res.shape[0]))

        # Make a partial function
        read_filter_partial = partial(read_filter, dataset, region['chrom'], region['start'], region['end'])

        # Time the partial function
        t = timeit.Timer(read_filter_partial).timeit(number=n_rep)
        print('Time taken to do {0} iterations: {1:.3f} secs\n'.format(n_rep, t))

    # Test each dataset, querying with (chrom) only
    print('\n#\n# Querying with (chrom) only\n#\n')
    for dataset in glob(os.path.join(in_dir, '*')):

        name = os.path.split(dataset)[1]
        print('# {0}'.format(name))

        # Load once to see how many rows are loaded with given filter
        res = read_filter(dataset, region['chrom'])
        print('Number of rows read: {0}'.format(res.shape[0]))

        # Make a partial function
        read_filter_partial = partial(read_filter, dataset, region['chrom'])

        # Time the partial function
        t = timeit.Timer(read_filter_partial).timeit(number=n_rep)
        print('Time taken to do {0} iterations: {1:.3f} secs\n'.format(n_rep, t))

    return 0

def read_filter(path, chrom=None, start=None, end=None):
    ''' Reads a specific chrom:start-end range from a sumstat parquet file
    '''
    # Construct filters
    filters = []
    if chrom:
        filters.append(('chrom', '==', chrom))
    if start:
        filters.append(('pos', '>=', start))
    if end:
        filters.append(('pos', '<=', end))

    # Filters only apply row-group filtering, not row filtering
    res = pd.read_parquet(
        path,
        engine='fastparquet',
        filters=filters
    )

    # Also need to apply the filter in pandas, but I don't want to test this
    # res['chrom'] = res['chrom'].astype(str)
    # res = res.loc[((res.chrom == chrom) & (res.pos >= start) & (res.pos <= end)), :]

    return res


if __name__ == '__main__':

    main()
