import psutil
import time
import pandas as pd
import polars as pl
import os


# LOAD
def load_data(pvp): # add --data=...
    path = os.path.join(os.getcwd(), 'benchmarks')
    if pvp == "pd":
        df = pd.read_parquet(os.path.join(path, 'panda.parquet'))
    elif pvp == "pl":
        df = pl.read_parquet(os.path.join(path, 'polar.parquet'))
    return df


# QUERY
def query(join_function, *args):
    res = join_function(*args)
    return res


# CREATE DATAFRAME
def create_benchmarks_df(res_benchmark):
    res = res_benchmark
    data = []
    
    data.append({
            'start': res['start_time'],
            'end': res['end_time'],
            'resource': 'time',
            'diff': res['end_time'] - res['start_time']
        })
    data.append({
            'start': res['start_cpu_percent'],
            'end': res['end_cpu_percent'],
            'resource': 'cpu',
            'diff': res['end_cpu_percent'] - res['start_cpu_percent']
        })
    data.append({
            'start': res['start_memory_mb'],
            'end': res['end_memory_mb'],
            'resource': 'memory',
            'diff': res['end_memory_mb'] - res['start_memory_mb']
        })
    res = pd.DataFrame(data)
    return res


# TRACK RESOURCE USAGE w/ psutil
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
    result = query

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
        "end_memory_mb":end_memory_mb,
        "result": result
    }
    
    
# PRINT RESULTS OF TRACK_CPU_MEMORY() (DEPRECATED)
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














## AGG
def agg_pandas(pandas):
    #pd_agg = pandas[pandas.columns[7:]].agg(fagg)
    pd_agg = pandas.describe()
    return pd_agg

def agg_polars(polars):
    #pl_agg = df[included_columns].agg(fagg)
    pl_agg = polars.summary()
    return pl_agg