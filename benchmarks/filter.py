from memory_profiler import profile
import benchmarks


# READ
pandas=benchmarks.read_pandas('pandas')
polars=benchmarks.read_polars('polars')

# FILTER
@profile
def query_pd():
    return benchmarks.filter_pandas(pandas)

@profile
def query_pl():
    return benchmarks.filter_polars(polars)


if __name__ == '__main__':
    res_pandas = benchmarks.track_cpu_memory(query_pd)
    res_polars = benchmarks.track_cpu_memory(query_pl)
    
    benchmarks.print_res(res_pandas, res_polars)
    
    # print(query_pd().head())
    # print(query_pl().head())

# write to csv
