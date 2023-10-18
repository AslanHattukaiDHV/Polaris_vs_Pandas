# polars_benchmark.py
import polars as pl
from memory_profiler import profile
import time

@profile
def read_polars():
    pl.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/polars.parquet')
    time.sleep(1)


if __name__ == '__main__':
    # Warm-up
    for _ in range(3):
        read_polars()
    
    # Measure memory and time
    tmp=read_polars()