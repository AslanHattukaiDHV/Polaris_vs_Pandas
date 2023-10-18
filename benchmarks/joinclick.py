import click
import pandas as pd
import polars as pl
import benchmarks
import os
from memory_profiler import profile

@click.command()
@click.option("--join", type=click.Choice(["pd", "pl"]), required=True, help="Select the join operation (valid choices: pandas or polars).")
def do_join(join):
    # PREP
    ## Read
    path = os.getcwd()
    panda = pd.read_parquet(os.path.join(path, 'benchmarks', 'panda.parquet'))
    polar = pl.read_parquet(os.path.join(path, 'benchmarks', 'polar.parquet'))

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
    def join_polars(polar, countries_pl): # add left, inner, outer joins
        pl_join = polar.join(countries_pl, on='Country')
        return pl_join

    # QUERY
    def query(join_function, *args):
        result = join_function(*args)
        return result

    if join == "pd":
        res = benchmarks.track_cpu_memory(query(join_pandas, panda, countries_pd))
    elif join == "pl":
        res = benchmarks.track_cpu_memory(query(join_polars, polar, countries_pl))
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
    do_join()