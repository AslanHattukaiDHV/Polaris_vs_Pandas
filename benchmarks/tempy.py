
import click
import pandas as pd
import polars as pl
import benchmarks
import os
from memory_profiler import profile


path = os.getcwd()
print(path)
panda = pd.read_parquet(os.path.join(path, 'benchmarks', 'panda.parquet'))
polar = pl.read_parquet(os.path.join(path, 'benchmarks', 'polar.parquet'))
print(panda.head())
print(polar.head())