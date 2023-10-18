import click
import pandas as pd
import polars as pl
import benchmarks
import os
from memory_profiler import profile

@click.command()
@click.option("--join", type=click.Choice(["pd", "pl"]), required=True, help="Select the join operation (valid choices: pandas or polars).")
def do_filter(join):
    # PREP
    ## Read
    path = os.getcwd()
    panda = pd.read_parquet(os.path.join(path, 'benchmarks', 'panda.parquet'))
    polar = pl.read_parquet(os.path.join(path, 'benchmarks', 'polar.parquet'))


    # QUERIES
    ## FILTER
    @profile
    def filter_pandas(panda):
        pd_filter = panda[(panda['Country'] == 'Belgium') &
                        (panda['Gender'] == 'Male') &
                        (panda['int_columncolumn_1'] > 30)]
        return pd_filter
    
    @profile
    def filter_polars(polar):
        pl_filter = polar.filter(pl.col('Country') == 'Belgium')
        return pl_filter
    
    # QUERY
    def query(join_function, *args):
        result = join_function(*args)
        return result
    
    if join == "pd":
        res = benchmarks.track_cpu_memory(query(filter_pandas, panda))
    elif join == "pl":
        res = benchmarks.track_cpu_memory(query(filter_polars, polar))
    else:
        click.echo("Invalid join option. Use --join pandas or --join polars.")
        return
    
    data = []
    
    data.append({
            'start': res['start_time'],
            'end': res['end_time'],
            'resource': 'time'
        })
    data.append({
            'start': res['start_cpu_percent'],
            'end': res['end_cpu_percent'],
            'resource': 'cpu'
        })
    data.append({
            'start': res['start_memory_mb'],
            'end': res['end_memory_mb'],
            'resource': 'memory'
        })
        
    res = pd.DataFrame(data)
    click.echo(res)
    
if __name__ == '__main__':
    do_filter()