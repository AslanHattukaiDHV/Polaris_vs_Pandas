# pandas_benchmark.py
import pandas as pd
from memory_profiler import profile
import time

@profile
def read_pandas():
    pd.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/pandas.parquet')
    time.sleep(1)

if __name__ == '__main__':
    # Warm-up
    for _ in range(3):
        read_pandas()
    
    # Measure memory and time
    tmp=read_pandas()