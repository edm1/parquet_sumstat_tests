#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import dask.dataframe as dd
from functools import partial
import timeit

def main():

    # Args
    in_path = sys.argv[1]
    n_rep=3

    datasets = [{
        'study_id': 'GTEX7',
        'cell_id': 'UBERON_0000178',
        'group_id': 'ENSG00000186715',
        'trait_id': 'eqtl',
        'chrom': '1',
        'start': 25186149,
        'end': 29186149
    }]

    for i, dataset in enumerate(datasets):

        print(i + 1)

        # Load once to see how many rows are loaded with given filter
        res = load_sumstats(in_path, dataset['study_id'], dataset['cell_id'], dataset['group_id'], dataset['trait_id'], dataset['chrom'], dataset['start'], dataset['end'])
        print('Number of rows read: {0}'.format(res.shape[0]))

        # Make a partial function
        read_filter_partial = partial(load_sumstats, in_path, dataset['study_id'], dataset['cell_id'], dataset['group_id'], dataset['trait_id'], dataset['chrom'], dataset['start'], dataset['end'])

        # Time the partial function
        t = timeit.Timer(read_filter_partial).timeit(number=n_rep)
        print('Time taken per iteration ({0}): {1:.3f} secs\n'.format(n_rep, float(t)/n_rep))


    return 0

def load_sumstats(in_pq, study_id, cell_id=None, group_id=None, trait_id=None,
                  chrom=None, start=None, end=None, build='b37'):
    ''' Loads summary statistics
    '''

    # Create row-group filters
    row_grp_filters = [('study_id', '==', study_id)]
    if cell_id:
        row_grp_filters.append(('cell_id', '==', cell_id))
    if group_id:
        row_grp_filters.append(('group_id', '==', group_id))
    if trait_id:
        row_grp_filters.append(('trait_id', '==', trait_id))
    if chrom:
        row_grp_filters.append(('chrom', '==', str(chrom)))
    if start:
        row_grp_filters.append(('pos_{}'.format(build), '>=', start))
    if end:
        row_grp_filters.append(('pos_{}'.format(build), '<=', end))

    # Read file
    df = dd.read_parquet(in_pq,
                         filters=row_grp_filters,
                         engine='fastparquet')

    # Conversion to in-memory pandas
    df = df.compute(scheduler='single-threaded')

    return df


if __name__ == '__main__':

    main()
