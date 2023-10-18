from memory_profiler import profile
import benchmarks
import pandas as pd
import polars as pl

@profile
def read_pandas():
    pd_read=pd.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/pandas.parquet')

@profile
def read_polars():
    pl_read=pl.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/polars.parquet')
        
if __name__ == '__main__':
    res_pd = benchmarks.track_cpu_memory(read_pandas)
    res_pl = benchmarks.track_cpu_memory(read_polars)

    benchmarks.print_res(res_pd, res_pl)