from memory_profiler import profile
import pandas as pd
import polars as pl
import time
import benchmarks
import argparse
import os

# Argument parser
parser = argparse.ArgumentParser(description="Choose join operation (pandas or polars).")
parser.add_argument("--join", choices=["pd", "pl"], required=True, help="Select the join operation (valid choices: pandas or polars).")
args = parser.parse_args() # https://stackoverflow.com/questions/42249982/systemexit-2-error-when-calling-parse-args-within-ipython

# PREP
## Read
path = os.getcwd()
panda = pd.read_parquet(path + '/benchmarks/panda.parquet')
polar = pd.read_parquet(path + '/benchmarks/polar.parquet')

#pandas = benchmarks.read_pandas('pandas')
#polars = benchmarks.read_polars('polars')

# QUERIES
countries_pd = panda.drop_duplicates(subset='Country', keep='first')
countries_pl = pl.DataFrame(countries_pd)

## pd
@profile
def join_pandas(panda, countries_pd):
    pd_join = panda.merge(countries_pd, on='Country')
    return pd_join

## pl
@profile
def join_polars(polar, countries_pl):
    pl_join = polar.join(countries_pl, on='Country')
    return pl_join

# QUERY
def query(join_function, *args):
    result = join_function(*args)
    return result

if __name__ == '__main':
    if args.join == "pd":
        res = query(join_pandas, panda, countries_pd)
    elif args.join == "pl":
        res = query(join_polars, polar, countries_pl)
    else:
        print("Invalid join option. Use --join pandas or --join polars.")
        exit(1)

    print(res.head())