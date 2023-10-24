import pandas as pd
import polars as pl
import click
from memory_profiler import profile
import reading
from pympler import asizeof
import sys


## FILTER queries for pvp
@profile
def filter_pandas(panda):
    pd_filter = panda[(panda['Country'] == 'Belgium') &
                    (panda['Gender'] == 'Male') &
                    (panda['int_columncolumn_1'] > 30)]
    return pd_filter

@profile
def filter_polars(polar):
    pl_filter = polar.filter((pl.col('Country') == 'Belgium') & 
                            (pl.col('Gender') == 'Male') & 
                            (pl.col('int_columncolumn_1') > 30))
    return pl_filter
    
    
@click.command()
@click.option("--pvp", type=click.Choice(["pd", "pl", "both"]), default='pd', required=True, help="Select the library to benchnmark (valid choices: pd (pandas) or pl (polars)).")
@click.option("--dataset", required=True, help="Select the dataset.")
def test_filter(pvp, dataset):
    if pvp=='pd':
        df=pd.read_parquet('benchmarks/datasets/'+dataset)
        #df=reading.read_pandas('benchmarks/datasets/'+dataset)
        res=filter_pandas(df)
        
    elif pvp=='pl':
        df=pl.read_parquet('benchmarks/datasets/'+dataset)
        #df=reading.read_polars('benchmarks/datasets/'+dataset)
        res=filter_polars(df)
    else:
        click.echo("Invalid pvp option. Use --pvp pd or --pvp pl.")
        return
    
    # click.echo(df)
    # click.echo(f"Size of the {pvp}.DataFrame object (sys): {sys.getsizeof(df)} bytes")
    # click.echo(f"Size of the {pvp}.DataFrame object (pympler): {asizeof.asizeof(df)} bytes")
    
    
if __name__=='__main__':
    test_filter()