import benchmarks.write_benchmark as write_benchmark
import pandas as pd
import polars as pl



if __name__=='__main__':
    
    # datasets for joining
    data = write_benchmark.generate_test_data(10000, 5, 5)
    panda = pd.DataFrame(data)

    countries_1 = panda.drop_duplicates(subset=['Country'], keep='first')
    countries_2 = panda.drop_duplicates(subset=['Country', 'Code'], keep='first') # just read next time from folder instead of manipulating
    # countries_pl_1 = pl.DataFrame(countries_pd_1)
    # countries_pl_2 = pl.DataFrame(countries_pd_2)

    countries_1.to_parquet("benchmarks/datasets/countries.parquet")
    countries_2.to_parquet("benchmarks/datasets/countries_code.parquet")
    # countries_pl_1.write_parquet("benchmarks/datasets/countries_pl_1.parquet")
    # countries_pl_2.write_parquet("benchmarks/datasets/countries_pl_2.parquet")