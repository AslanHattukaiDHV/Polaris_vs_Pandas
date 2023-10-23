import click
import pandas as pd
import polars as pl
import benchmarks
import os
from memory_profiler import profile
import sys
from pympler import asizeof


@click.command()
@click.option("--pvp", type=click.Choice(["pd", "pl"]), required=True, help="Select the library to benchnmark (valid choices: pd (pandas) or pl (polars)).")
@click.option("--df_path", default=os.path.join(os.path.join(os.getcwd(), 'benchmarks/')), required=True, help="Specify path to the dataset.")
@click.option("--df_name", required=True, help="Specify the name of the dataset.")
@click.option("--type_join", type=click.Choice(["left", "inner", "outer"]), default="inner", required=True, help="Specify the type of join.")
@click.option("--join_on", type=click.Choice(["country", "country_code"]), default="country", required=True, help="Specify the columns to join on.")
def do_join(pvp, df_path, df_name, type_join, join_on):
    
    # PREP
    ## Read
    #df = benchmarks.load_data(pvp) # add
    #df = pd.read_parquet(os.path.join(os.path.join(os.getcwd(), 'benchmarks'), 'panda_join.parquet'))
    df = pd.read_parquet(df_path + df_name)
    
    if pvp == "pd":
        countries_pd = pd.read_parquet(os.path.join(os.path.join(os.getcwd(), 'benchmarks'), 'panda_join_C.parquet'))
    elif pvp == "pl":
        countries_pl = pl.read_parquet(os.path.join(os.path.join(os.getcwd(), 'benchmarks'), 'polar_join.parquet'))
    
    click.echo(f"Size of the {pvp}.DataFrame object (sys): {sys.getsizeof(df)} bytes")
    click.echo(f"Size of the {pvp}.DataFrame object (pympler): {asizeof.asizeof(df)} bytes")

    ## JOIN queries for pvp
    ### columns to join on
    
    # if pvp == "pd":
        # countries_pd = df.drop_duplicates(subset='Country', keep='first') # just read next time from folder instead of manipulating
    # elif pvp == "pl":
        # df = pd.DataFrame(df)
        # countries_pd = df.drop_duplicates(subset='Country', keep='first') # just read next time from folder instead of manipulating
        # countries_pl = pl.DataFrame(countries_pd)

    ### pd
    @profile
    def join_pandas(panda, countries_pd):
        if join_on == "country":
            pd_join = panda.merge(countries_pd, on=['Country'], how=type_join)
        elif join_on == "country_code":
            pd_join = panda.merge(countries_pd, on=['Country', 'Code'], how=type_join)

        return pd_join

    ### pl
    @profile
    def join_polars(polar, countries_pl):
        if join_on == "country":
            pl_join = polar.join(countries_pl, on=['Country'],  how=type_join)
        elif join_on == "country_code":
            pl_join = polar.join(countries_pl, on=['Country', 'Code'], how=type_join)
            
        return pl_join
    
    
    # # TEST pandas or polars?
    # if pvp == "pd":
    #     res = benchmarks.track_cpu_memory(benchmarks.query(join_pandas, df, countries_pd))
    # elif pvp == "pl":
    #     res = benchmarks.track_cpu_memory(benchmarks.query(join_polars, df, countries_pl))
    # else:
    #     click.echo("Invalid pvp option. Use --pvp pd or --pvp pl.")
    #     return
    
    
    # # CREATE DATAFRAME
    # res = benchmarks.create_benchmarks_df(res)
    
    
    
    
    
    # TEST pandas or polars?
    if pvp == "pd":
        res = benchmarks.query(join_pandas, df, countries_pd)
    elif pvp == "pl":
        res = benchmarks.query(join_polars, df, countries_pl)
    else:
        click.echo("Invalid pvp option. Use --pvp pd or --pvp pl.")
        return
    
    
    # RETURN
    click.echo(res)
    
if __name__ == '__main__':
    do_join()