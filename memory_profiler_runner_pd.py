
# importing the library
from memory_profiler import profile
import pandas as pd 
import data_creation



@profile
def test_read_pd():
    for data_set_location in data_creation.get_all_datasets():
      df_loaded = pd.read_parquet(data_set_location)


 
if __name__ == '__main__':
    test_read_pd()