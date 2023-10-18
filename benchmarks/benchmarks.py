import psutil
import time
import pandas as pd
import polars as pl

def track_cpu_memory(query, *args):
    cpu_usage_perc = []
    memory_usage_mb = []

    # start
    process = psutil.Process()
    start_time = time.time()
    start_cpu_percent = process.cpu_percent(interval=None)
    start_cpu_percent = process.cpu_percent(interval=None)
    start_memory_mb = process.memory_info().rss / (1024 * 1024)

    # run query
    result = query()

    # finish
    end_time = time.time()
    total_time = end_time - start_time
    end_cpu_percent = process.cpu_percent(interval=None)
    end_memory_mb = process.memory_info().rss / (1024 * 1024)

    # calculate cpu & memory
    cpu_usage_perc.append(end_cpu_percent - start_cpu_percent)
    memory_usage_mb.append(end_memory_mb - start_memory_mb)

    return {
        "avg_cpu_usage": cpu_usage_perc,
        "avg_memory_usage_mb": memory_usage_mb,
        "total_time": total_time,
        "start_time": start_time,
        "end_time": end_time,
        "start_cpu_percent": start_cpu_percent,
        "end_cpu_percent": end_cpu_percent,
        "start_memory_mb": start_memory_mb,
        "end_memory_mb":end_memory_mb
    }
    
def print_res(res_pd, res_pl):
    num_cores = psutil.cpu_count()
    print(f"Number of cpu cores: {num_cores}")
    print(f"Average cpu usage: \n\t pandas: {res_pd['avg_cpu_usage']}% \n\t polars: {res_pl['avg_cpu_usage']}% ({res_pl['avg_cpu_usage'][0]/num_cores:.2f}%/core)")
    print(f"Average memory usage: \n\t pandas: {res_pd['avg_memory_usage_mb']} MB \n\t polars: {res_pl['avg_memory_usage_mb']} MB")
    print(f"Total duration: \n\t pandas: {res_pd['total_time']} seconds \n\t polars: {res_pl['total_time']} seconds")
    print(f"Start cpu [{res_pd['start_cpu_percent']}], end cpu [{res_pd['end_cpu_percent']}]")

def warm_up(functie, warmup):
    for _ in range(warmup):
        functie()


# QUERIES
## READ
def read_pandas(file_name):
    pd_read=pd.read_parquet(f'./benchmarks/{file_name}.parquet')
    return pd_read

def read_polars(file_name):
    pl_read=pl.read_parquet(f'./benchmarks/{file_name}.parquet')
    return pl_read

## FILTER
def filter_pandas(pandas):
    pd_filter = pandas[(pandas['Country'] == 'Belgium') &
                       (pandas['Gender'] == 'Male') &
                       (pandas['int_columncolumn_1'] > 30)]
    return pd_filter

def filter_polars(polars):
    pl_filter = polars.filter((pl.col('Country') == 'Belgium') &
                              (pl.col('Gender') == 'Male') &
                              (pl.col('int_columncolumn_1') > 30))
    return pl_filter


## JOIN
def join_pandas(pandas, countries_pd):
    pd_join = pandas.merge(countries_pd, on='Country')
    return pd_join

def join_polars(polars, countries_pl):
    pl_join = polars.join(countries_pl, on='Country')
    return pl_join




## AGG
def agg_pandas(pandas):
    #pd_agg = pandas[pandas.columns[7:]].agg(fagg)
    pd_agg = pandas.describe()
    return pd_agg

def agg_polars(polars):
    #pl_agg = df[included_columns].agg(fagg)
    pl_agg = polars.summary()
    return pl_agg