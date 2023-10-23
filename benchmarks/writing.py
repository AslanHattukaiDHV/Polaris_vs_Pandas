import pandas as pd
import polars as pl
import click
import random
import string
from memory_profiler import profile
import psutil
import time


def generate_test_data(num_rows=100000, num_float_cols=11, num_int_cols=11):
    # Generate CRM data
    european_countries = [
    'Albania', 'Andorra', 'Armenia', 'Austria', 'Azerbaijan', 'Belarus', 'Belgium', 'Bosnia and Herzegovina',
    'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Georgia',
    'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kazakhstan', 'Kosovo', 'Latvia', 'Liechtenstein',
    'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Monaco', 'Montenegro', 'Netherlands', 'North Macedonia',
    'Norway', 'Poland', 'Portugal', 'Romania', 'Russia', 'San Marino', 'Serbia', 'Slovakia', 'Slovenia', 'Spain',
    'Sweden', 'Switzerland', 'Turkey', 'Ukraine', 'United Kingdom', 'Vatican City']

    data = {
        'CustomerID': range(1, num_rows + 1),
        'Name': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(num_rows)],
        'Email': [''.join(random.choices(string.ascii_lowercase, k=7)) + '@example.com' for _ in range(num_rows)],
        'Phone': [''.join(random.choices(string.digits, k=10)) for _ in range(num_rows)],
        'Address': [''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in range(num_rows)],
        'Country': [random.choice(european_countries) for _ in range(num_rows)],
        'Code': [''.join(random.choices(string.digits, k=2)) for _ in range(num_rows)],
        'Gender': [random.choice(['Male', 'Female']) for _ in range(num_rows)],
    }

    # Add random integer columns
    for i in range(1, num_float_cols): # adjust range to increase amount of cols
        column_name = str(f'int_columncolumn_{i}')
        data[column_name] = random.choices(range(100), k=num_rows)

    # Add random float columns
    for i in range(1, num_int_cols): # adjust range to increase amount of cols
        column_name = str(f'float_column_{i}')
        data[column_name] = [random.uniform(0, 1) for _ in range(num_rows)]
        
    return data 


@profile
def write_to_parquet_pd(data, filename):
    df = pd.DataFrame(data)
    df.to_parquet("benchmarks/datasets/"+filename)

@profile
def write_to_parquet_pl(data, filename):
    df = pl.DataFrame(data)
    df.write_parquet("benchmarks/datasets/"+filename)


@click.command()
@click.option("--pvp", type=click.Choice(["pd", "pl", "both"]), default='both', required=True, help="Select the library to benchnmark (valid choices: pd (pandas) or pl (polars)).")
@click.option("--num_rows", default=1000000, required=True, help="Choose number of rows.")
@click.option("--num_int_cols", default=5, required=True, help="Choose number of integer columns.")
@click.option("--num_float_cols", default=5, required=True, help="Choose number of float columns.")
def test_write(pvp, num_rows, num_int_cols, num_float_cols):
    
    data = generate_test_data(num_rows, num_int_cols, num_float_cols)
    
    if pvp=='pd':
        write_to_parquet_pd(data, 'tmp_pd.parquet')
    elif pvp=='pl':
        write_to_parquet_pl(data, 'tmp_pl.parquet')
    elif pvp=='both':
        write_to_parquet_pd(data, 'panda.parquet')
        write_to_parquet_pl(data, 'polar.parquet')
        
        panda = pd.DataFrame(data)
        
        countries_pd_1 = panda.drop_duplicates(subset=['Country'], keep='first')
        countries_pd_2 = panda.drop_duplicates(subset=['Country', 'Code'], keep='first') # just read next time from folder instead of manipulating
        countries_pl_1 = pl.DataFrame(countries_pd_1)
        countries_pl_2 = pl.DataFrame(countries_pd_2)
        
        countries_pd_1.to_parquet("benchmarks/datasets/countries_pd_1.parquet")
        countries_pd_2.to_parquet("benchmarks/datasets/countries_pd_2.parquet")
        countries_pl_1.write_parquet("benchmarks/datasets/countries_pl_1.parquet")
        countries_pl_2.write_parquet("benchmarks/datasets/countries_pl_2.parquet")
    else:
        click.echo("Invalid pvp option. Use --pvp pd, --pvp pl or --pvp both.")
        return
    
if __name__=='__main__':
    cpu_usage_perc = []
    memory_usage_mb = []
    process = psutil.Process()
    start_time = time.time()
    start_cpu_percent = process.cpu_percent(interval=None)
    start_cpu_percent = process.cpu_percent(interval=None)
    start_memory_mb = process.memory_info().rss / (1024 * 1024)
    
    test_write()
    
    end_time = time.time()
    total_time = end_time - start_time
    end_cpu_percent = process.cpu_percent(interval=None)
    end_memory_mb = process.memory_info().rss / (1024 * 1024)

    # calculate cpu & memory
    cpu_usage_perc.append(end_cpu_percent - start_cpu_percent)
    memory_usage_mb.append(end_memory_mb - start_memory_mb)