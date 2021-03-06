# parquet_sumstat_tests
Scripts to test optimise parquet format for sum stats

Sorting/partitioning methods:
1. Sort by (chrom, pos) and write without partitioning on any columns
2. Sort by (chrom) and write without partitioning on any columns
3. Sort by (pos) and write without partitioning on any columns
4. Sort by (pos) and partition on chrom columns

## Setup

```
bash setup.sh
conda env create -n parquet_test --file environment.yaml
source activate parquet_test
```

## Output/results from optimisation

```
#
# Querying by (chrom, start, end)
#

# chrom-pos-sort_no-partitioning
Number of rows read: 299817
Time taken to do 10 iterations: 2.367 secs

# chrom-sort_no-partitioning
Number of rows read: 999390
Time taken to do 10 iterations: 7.258 secs

# pos-sort_no-partitioning
Number of rows read: 99939
Time taken to do 10 iterations: 1.028 secs

# pos-sort_chrom-partitioning
Number of rows read: 5518
Time taken to do 10 iterations: 10.721 secs

#
# Querying by (chrom) only
#

# chrom-pos-sort_no-partitioning
Number of rows read: 999390
Time taken to do 10 iterations: 6.959 secs

# chrom-sort_no-partitioning
Number of rows read: 999390
Time taken to do 10 iterations: 7.045 secs

# pos-sort_no-partitioning
Number of rows read: 12792192
Time taken to do 10 iterations: 104.503 secs

# pos-sort_chrom-partitioning
Number of rows read: 892038
Time taken to do 10 iterations: 16.961 secs
```

## Output/results from dask gcsfuse speed test

On Google compute instance
```
Testing local
Number of rows read: 2135592
Time taken per iteration (3): 5.920 secs

Testing gcs
Number of rows read: 2135592
Time taken per iteration (3): 7.490 secs
```

On my mac (on EBI network)
```
Testing local
Number of rows read: 2135592
Time taken per iteration (3): 4.831 secs

Testing gcs
Number of rows read: 2135592
Time taken per iteration (3): 14.255 secs
```
