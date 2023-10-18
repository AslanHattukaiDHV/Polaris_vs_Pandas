from memory_profiler import profile
import benchmarks
import pandas as pd
import polars as pl
import time


# JOIN
@profile
def query_pd():
    return benchmarks.join_pandas(pandas, countries_pd)

@profile
def query_pl():
    return benchmarks.join_polars(polars, countries_pl)


if __name__ == '__main__':
    res_pandas = benchmarks.track_cpu_memory(query_pd)
    time.sleep(2)
    res_polars = benchmarks.track_cpu_memory(query_pl)
    time.sleep(2)
    
    benchmarks.print_res(res_pandas, res_polars)

# write to csv