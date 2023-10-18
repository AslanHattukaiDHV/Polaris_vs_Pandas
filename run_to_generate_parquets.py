import data_creation
import pandas as pd
import polars as pl

data = data_creation.generate_test_data(10000)
panda = pd.DataFrame(data)
polar = pl.DataFrame(data)

panda.to_parquet("benchmarks/panda.parquet")
polar.write_parquet("benchmarks/polar.parquet")
