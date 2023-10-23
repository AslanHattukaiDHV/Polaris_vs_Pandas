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
def test_filter(pvp):
    if pvp=='pd':
        df=reading.read_pandas(filepath='benchmarks/datasets/panda.parquet')
        res=filter_pandas(df)
        
    elif pvp=='pl':
        df=reading.read_polars(filepath='benchmarks/datasets/polar.parquet')
        res=filter_polars(df)
    else:
        click.echo("Invalid pvp option. Use --pvp pd or --pvp pl.")
        return
    
    click.echo(df)
    click.echo(f"Size of the {pvp}.DataFrame object (sys): {sys.getsizeof(df)} bytes")
    click.echo(f"Size of the {pvp}.DataFrame object (pympler): {asizeof.asizeof(df)} bytes")
    
    
if __name__=='__main__':
    test_filter()