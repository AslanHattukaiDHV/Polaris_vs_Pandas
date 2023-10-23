import pandas as pd
import polars as pl
import click
from memory_profiler import profile
import reading


## FILTER queries for pvp
@profile
def agg_pandas(panda):
    pd_agg = panda.groupby('Country').agg({
        'int_columncolumn_1': ['sum', 'max'],
        'float_column_1': ['sum', 'max'],
        'int_columncolumn_2': ['sum', 'max'],
        'float_column_2': ['sum', 'max']
        })
    return pd_agg

@profile
def agg_polars(polar):
    pl_agg = polar.groupby('Country').agg(
        pl.sum('int_columncolumn_1').alias('sum_int_1'),
        pl.sum('float_column_1').alias('sum_float_1'),
        pl.max('int_columncolumn_1').alias('max_int_1'),
        pl.max('float_column_1').alias('max_float_1'),
        pl.sum('int_columncolumn_2').alias('sum_int_2'),
        pl.sum('float_column_2').alias('sum_float_2'),
        pl.max('int_columncolumn_2').alias('max_int_2'),
        pl.max('float_column_2').alias('max_float_2')
        )
    return pl_agg
    
    
@click.command()
@click.option("--pvp", type=click.Choice(["pd", "pl"]), default='pd', required=True, help="Select the library to benchnmark (valid choices: pd (pandas) or pl (polars)).")
def test_agg(pvp):
    if pvp=='pd':
        df=reading.read_pandas(filepath='benchmarks/datasets/panda.parquet')
        res=agg_pandas(df)
        
    elif pvp=='pl':
        df=reading.read_polars(filepath='benchmarks/datasets/polar.parquet')
        res=agg_polars(df)
    else:
        click.echo("Invalid pvp option. Use --pvp pd or --pvp pl.")
        return
    
    click.echo(df)
    click.echo(res)
    
    
if __name__=='__main__':
    test_agg()