
import pandas as pd
import random
import string 

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

def write_to_parquet(data, filename):
    df = pd.DataFrame(data)
    df.to_parquet("data/"+filename)