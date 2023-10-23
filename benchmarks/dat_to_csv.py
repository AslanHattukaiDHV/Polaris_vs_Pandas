import pandas as pd

file_path = 'mprofile_20231023135950.dat'
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

# Print the DataFrame
print(df)
