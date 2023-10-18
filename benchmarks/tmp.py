import join
import benchmarks
import polars as pl
import pandas as pd

# READ
pandas=benchmarks.read_pandas('pandas')
polars=benchmarks.read_polars('polars')

countries_pd = pandas.drop_duplicates(subset='Country', keep='first')
countries_pl = pl.DataFrame(countries_pd)

res = join.query_pd()