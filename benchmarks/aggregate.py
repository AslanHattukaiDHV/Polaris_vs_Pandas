from memory_profiler import profile
import benchmarks
import pandas as pd
import polars as pl
import time


# READ
pandas=benchmarks.read_pandas('pandas')
polars=benchmarks.read_polars('polars')


# JOIN
@profile
def query_pd():
    return benchmarks.agg_pandas(pandas)

@profile
def query_pl():
    return benchmarks.agg_polars(polars)


if __name__ == '__main__':
    # start_time = []
    # end_time = []
    # start_cpu = []
    # end_cpu = []
    # start_mem = []
    # end_mem = []
    data = []
    
    print(f"start - end")
    for i in range(2):
        res_pandas = benchmarks.track_cpu_memory(query_pd)
        print(f"time: {res_pandas['start_time']} - {res_pandas['end_time']}")
        print(f"cpu: {res_pandas['start_cpu_percent']} - {res_pandas['end_cpu_percent']}")
        print(f"ram: {res_pandas['start_memory_mb']} - {res_pandas['end_memory_mb']}")
        
        data.append({
            'start': res_pandas['start_time'],
            'end': res_pandas['end_time'],
            'resource': 'time'
        })
        data.append({
            'start': res_pandas['start_cpu_percent'],
            'end': res_pandas['end_cpu_percent'],
            'resource': 'cpu'
        })
        data.append({
            'start': res_pandas['start_memory_mb'],
            'end': res_pandas['end_memory_mb'],
            'resource': 'memory'
        })
        res_pandas_benchmark = pd.DataFrame(data)
        res_pandas_benchmark['step'] = i
        res_pandas_benchmark['pvp'] = 'pandas'
        

    #     start_time.append(res_pandas['start_time'])
    #     end_time.append([res_pandas['end_time']])
    #     resource.append(['time', 'cpu', 'mem'])
    #     start_cpu.append(res_pandas['start_cpu_percent'])
    #     end_cpu.append(res_pandas['end_cpu_percent'])
    #     start_mem.append(res_pandas['start_memory_mb'])
    #     end_mem.append(res_pandas['end_memory_mb'])

    # res_pandas_benchmark = pd.DataFrame({
    #     'Start Time': start_time,
    #     'End Time': end_time,
    #     'Start CPU %': start_cpu,
    #     'End CPU %': end_cpu,
    #     'Start Memory (MB)': start_mem,
    #     'End Memory (MB)': end_mem
    # })
    
    time.sleep(1)

    # Print the DataFrame
    print(res_pandas_benchmark)
    # time.sleep(5)
    # res_polars = benchmarks.track_cpu_memory(query_pl)
    # time.sleep(5)
    
    # benchmarks.print_res(res_pandas, res_polars)

# write to csv