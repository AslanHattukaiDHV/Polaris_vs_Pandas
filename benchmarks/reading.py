import pandas as pd
import polars as pl
import click
from memory_profiler import profile

# def find_files_with_prefix(directory, prefix):
#     matching_files = []
#     for file_name in os.listdir(directory):
#         if file_name.startswith(prefix):
#             matching_files.append(file_name)
#     return matching_files

# def get_all_datasets():
#     directory = "data"
#     prefix = "dataset_"
#     matching_files = find_files_with_prefix(directory, prefix)
#     matching_files_full_path = []
#     for file_name in matching_files:
#         matching_files_full_path.append("data/"+file_name)
#     return(matching_files_full_path)


@profile
def read_pandas(filepath):
    return pd.read_parquet(filepath)

@profile
def read_polars(filepath):
    return pl.read_parquet(filepath)


@click.command()
@click.option("--pvp", type=click.Choice(["pd", "pl", "both"]), default='both', required=True, help="Select the library to benchnmark (valid choices: pd (pandas) or pl (polars)).")
@click.option("--dataset", required=True, help="Select the dataset.")
def test_read(pvp, dataset):        
    if pvp=='pd':
        read_pandas('benchmarks/datasets/'+dataset)
    elif pvp=='pl':
        read_polars('benchmarks/datasets/'+dataset)
    elif pvp=='both':
        panda=read_pandas('benchmarks/datasets/'+dataset)
        polar=read_polars('benchmarks/datasets/'+dataset)
        return panda, polar
    else:
        click.echo("Invalid pvp option. Use --pvp pd, --pvp pl or --pvp both.")
        return
    

if __name__=='__main__':
    test_read()