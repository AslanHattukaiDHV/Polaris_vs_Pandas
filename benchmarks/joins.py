from memory_profiler import profile
import pandas as pd
import polars as pl
import time
import benchmarks

# PREP
## Read
pandas=benchmarks.read_pandas('pandas')
polars=benchmarks.read_polars('polars')
#pandas=pd.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/pandas.parquet')
#polars=pd.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/polars.parquet')



# QUERIES
countries_pd = pandas.drop_duplicates(subset='Country', keep='first')
countries_pl = pl.DataFrame(countries_pd)

## pd
@profile
def join_pandas(pandas, countries_pd):
    pd_join = pandas.merge(countries_pd, on='Country')
    return pd_join

## pl
@profile
def join_polars(polars, countries_pl):
    pl_join = polars.join(countries_pl, on='Country')
    return pl_join


# # CALL
# def query(join_type):
#     if join_type == "pandas":
#         return join_pandas(pandas, countries_pd)
#     elif join_type == "polars":
#         return join_polars(polars, countries_pl)
#     else:
#         return None  # Handle invalid join type

# if __name__ == '__main__':
#     res = query('pandas')
#     print(res.head())
    
    
# CALL
def query(join_function, *args):
    result = join_function(*args)
    return result


if __name__ == '__main__':
    res_pandas = query(join_pandas, pandas, countries_pd)
    #res_polars = query(join_polars, polars, countries_pl)
    print(res_pandas.head())
    print(res_polars.head())
    
    #benchmarks.print_res(res_pandas, res_polars)

# write to csv
