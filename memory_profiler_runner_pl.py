
# importing the library
from memory_profiler import profile
import polars as pl 
import data_creation

#mprof run
#mprof plot



@profile
def test_read_pl():
  
    for data_set_location in data_creation.get_all_datasets():
      df_loaded = pl.read_parquet(data_set_location)


 
if __name__ == '__main__':
    test_read_pl()