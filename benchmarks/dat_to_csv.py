import pandas as pd
import os
import butils

# def find_files_with_prefix(directory, prefix):
#     matching_files = []
#     for file_name in os.listdir(directory):
#         if file_name.startswith(prefix):
#             matching_files.append(file_name)
#     return matching_files

# def get_all_datasets(directory):
#     prefix = "mprofile_"
#     matching_files = find_files_with_prefix(directory, prefix)
#     matching_files_full_path = []
#     for file_name in matching_files:
#         matching_files_full_path.append(file_name)
#     return(matching_files_full_path)


def dat_to_csv(file_path):
        
    mem_usage_mib = []
    timestamp = []

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()

            if parts[0] == 'CMDLINE':
                cmdline = ' '.join(parts[1:])
                print(f'CMDLINE: {cmdline}')
                cmdline_file = ' '.join(parts[5:6])
                print(f'CMDLINE file: {cmdline_file}')
                cmdline_args = ' '.join(parts[6:])
                print(f'CMDLINE args: {cmdline_args}')
                
            elif parts[0] == 'CHLD':
                child_info = int(parts[1]), float(parts[2]), float(parts[3])
                print(f'CHLD: Child Info = {child_info}')
                mem_usage_mib.append(child_info[1])
                timestamp.append(child_info[2])
    
    # Create a DataFrame from the arrays
    df = pd.DataFrame({'mem_usage_mib': mem_usage_mib, 'timestamp': timestamp})

    df['cmdline_file'] = cmdline_file
    df['cmdline_args'] = cmdline_args
    
    return df
        
if __name__=='__main__':
    
    frames=[]
    
    datasets = butils.find_files_with_prefix('./', 'mprofile_')
    print(datasets)
    
    for dat in datasets:
        frame = dat_to_csv('./'+dat)
        frames.append(frame)
        print(frame.shape)
    res=pd.concat(frames)
    
    # save to csv
    res.to_csv(f'benchmarks/csv_benchmarks/all.csv') # index=False