import pandas as pd
import polars as pl
import click
from memory_profiler import profile


@profile
def read_pandas(filepath='benchmarks/datasets/panda.parquet'):
    return pd.read_parquet(filepath)

@profile
def read_polars(filepath='benchmarks/datasets/polar.parquet'):
    return pl.read_parquet(filepath)


@click.command()
@click.option("--pvp", type=click.Choice(["pd", "pl", "both"]), default='both', required=True, help="Select the library to benchnmark (valid choices: pd (pandas) or pl (polars)).")
def test_read(pvp):
    
    if pvp=='pd':
        read_pandas()
    elif pvp=='pl':
        read_polars()
    elif pvp=='both':
        panda=read_pandas()
        polar=read_polars()
        return panda, polar
    else:
        click.echo("Invalid pvp option. Use --pvp pd, --pvp pl or --pvp both.")
        return
    
if __name__=='__main__':
    test_read()