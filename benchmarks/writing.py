import pandas as pd
import polars as pl
import click
import random
import string
from memory_profiler import profile


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
        #write_to_parquet_pd(data, f"panda_{num_rows}rows_{num_int_cols}ints_{num_float_cols}floats.parquet")
        write_to_parquet_pl(data, f"dataset_{num_rows}rows_{num_int_cols}ints_{num_float_cols}floats.parquet")
    else:
        click.echo("Invalid pvp option. Use --pvp pd, --pvp pl or --pvp both.")
        return

    
if __name__=='__main__':    
    test_write()