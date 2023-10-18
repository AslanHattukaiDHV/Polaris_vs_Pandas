from memory_profiler import profile
import pandas as pd
import polars as pl
import time
import benchmarks
import argparse

# Argument parser
parser = argparse.ArgumentParser(description="Choose join operation (pandas or polars).")
parser.add_argument("--join", choices=["pd", "pl"], required=True, help="Select the join operation (valid choices: pandas or polars).")
args = parser.parse_args()

# PREP
## Read

pandas=pd.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/pandas.parquet')
polars=pd.read_parquet('/Users/IvanEsin/self/repos/Polaris_vs_Pandas/benchmarks/polars.parquet')

#pandas = benchmarks.read_pandas('pandas')
#polars = benchmarks.read_polars('polars')

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

# QUERY
def query(join_function, *args):
    result = join_function(*args)
    return result

if __name__ == '__main':
    if args.join == "pd":
        res = query(join_pandas, pandas, countries_pd)
    elif args.join == "pl":
        res = query(join_polars, polars, countries_pl)
    else:
        print("Invalid join option. Use --join pandas or --join polars.")
        exit(1)

    print(f"{args.join.capitalize()} Result:")
    print(res.head())
