from memory_profiler import profile
import random
import string
import pandas as pd
import polars as pl

@profile
def generate_random_data(num_rows=1000, num_int_cols=10, num_float_cols=10):
    european_countries = [
        'Albania', 'Andorra', 'Armenia', 'Austria', 'Azerbaijan', 'Belarus', 'Belgium', 'Bosnia and Herzegovina',
        'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Georgia',
        'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kazakhstan', 'Kosovo', 'Latvia', 'Liechtenstein',
        'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Monaco', 'Montenegro', 'Netherlands', 'North Macedonia',
        'Norway', 'Poland', 'Portugal', 'Romania', 'Russia', 'San Marino', 'Serbia', 'Slovakia', 'Slovenia', 'Spain',
        'Sweden', 'Switzerland', 'Turkey', 'Ukraine', 'United Kingdom', 'Vatican City'
    ]

    data = {
        'CustomerID': list(range(1, num_rows + 1)),
        'Name': [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(num_rows)],
        'Email': [''.join(random.choices(string.ascii_lowercase, k=7)) + '@example.com' for _ in range(num_rows)],
        'Phone': [''.join(random.choices(string.digits, k=10)) for _ in range(num_rows)],
        'Address': [''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in range(num_rows)],
        'Country': [random.choice(european_countries) for _ in range(num_rows)],
        'Gender': [random.choice(['Male', 'Female']) for _ in range(num_rows)]
    }

    data.update({f'Int{i}': [random.choices(range(100), k=num_rows) for _ in range(num_rows)] for i in range(1, num_int_cols + 1)})
    data.update({f'Float{i}': [random.uniform(0, 1) for _ in range(num_rows)] for i in range(1, num_float_cols + 1)})

    return data

# Generate CRM data
if __name__ == "__main__":
    data = generate_random_data()
    df_pl = pl.DataFrame(data)
    df_pd = pd.DataFrame(data)