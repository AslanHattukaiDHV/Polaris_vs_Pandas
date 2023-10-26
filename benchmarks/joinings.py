import pandas as pd
import polars as pl
import click
from memory_profiler import profile
import time


### pd
@profile
def join_pandas(panda, countries_pd, type_join, join_on):
    if join_on == "country":
        pd_join = panda.merge(countries_pd, on=['Country'], how=type_join)
    elif join_on == "country_code":
        pd_join = panda.merge(countries_pd, on=['Country', 'Code'], how=type_join)
    time.sleep(1) # so that the function does not return too soon and we get reliable results
    return pd_join

### pl
@profile
def join_polars(polar, countries_pl, type_join, join_on):
    if join_on == "country":
        pl_join = polar.join(countries_pl, on=['Country'],  how=type_join)
    elif join_on == "country_code":
        pl_join = polar.join(countries_pl, on=['Country', 'Code'], how=type_join)
    time.sleep(1) # so that the function does not return too soon and we get reliable results
    return pl_join
    
    
@click.command()
@click.option("--pvp", type=click.Choice(["pd", "pl"]), default='pd', required=True, help="Select the library to benchnmark (valid choices: pd (pandas) or pl (polars)).")
@click.option("--dataset", required=True, help="Select the dataset.")
@click.option("--type_join", type=click.Choice(["left", "inner", "outer"]), default="inner", required=True, help="Specify the type of join.")
@click.option("--join_on", type=click.Choice(["country", "country_code"]), default="country", required=True, help="Specify the columns to join on.")
def test_join(pvp, dataset, type_join, join_on):
    if pvp=='pd':
        df=pd.read_parquet('benchmarks/datasets/'+dataset)
        # df=reading.read_pandas(filepath='benchmarks/datasets/panda.parquet')
        if join_on == "country":
            df2=pd.read_parquet('benchmarks/datasets/countries.parquet')
        elif join_on == "country_code":
            df2=pd.read_parquet('benchmarks/datasets/countries_code.parquet')
        res=join_pandas(df, df2, type_join, join_on)
        
    elif pvp=='pl':
        df=pl.read_parquet('benchmarks/datasets/'+dataset)
        # df=reading.read_polars(filepath='benchmarks/datasets/polar.parquet')
        if join_on == "country":
            df2=pl.read_parquet('benchmarks/datasets/countries.parquet')
        elif join_on == "country_code":
            df2=pl.read_parquet('benchmarks/datasets/countries_code.parquet')
        res=join_polars(df, df2, type_join, join_on)
        
    else:
        click.echo("Invalid pvp option. Use --pvp pd or --pvp pl.")
        return
    
    # click.echo(df)
    # click.echo(df2)
    # click.echo(res)

    
    
if __name__=='__main__':
    test_join()