import subprocess


# run writing benchmarks
def run_writing(pvp, num_rows, num_int_cols, num_float_cols):
    command = [
        "mprof",
        "run",
        "-M",
        "./bears.py",
        f"--pvp",
        pvp,
        "writing"
        f"--num_rows",
        num_rows,
        f"--num_int_cols",
        num_int_cols,
        f"--num_float_cols",
        num_float_cols,
    ]
    
    print(command)
    subprocess.run(command)


# # run reading benchmarks
# def run_reading(pvp, dataset):
#     command = [
#         "mprof",
#         "run",
#         "-M",
#         "./benchmarks/reading.py",
#         f"--pvp",
#         pvp,
#         f"--dataset",
#         dataset,
#     ]

#     print(command)
#     subprocess.run(command)


# # run filter benchmarks
# def run_filtering(pvp, dataset):
#     command = [
#         "mprof",
#         "run",
#         "-M",
#         "./benchmarks/filtering.py",
#         f"--pvp",
#         pvp,
#         f"--dataset",
#         dataset,
#     ]

#     print(command)
#     subprocess.run(command)


# # run joinings benchmarks
# def run_joinings(pvp, dataset, type_join, join_on):
#     command = [
#         "mprof",
#         "run",
#         "-M",
#         "./benchmarks/joinings.py",
#         f"--pvp",
#         pvp,
#         f"--dataset",
#         dataset,
#         f"--type_join",
#         type_join,
#         f"--join_on",
#         join_on,
#     ]

#     print(command)
#     subprocess.run(command)


# # run aggregating benchmarks
# def run_aggregating(pvp, dataset):
#     command = [
#         "mprof",
#         "run",
#         "-M",
#         "./benchmarks/aggregating.py",
#         f"--pvp",
#         pvp,
#         f"--dataset",
#         dataset,
#     ]

#     print(command)
#     subprocess.run(command)


# if __name__ == "__main__":
#     bears(auto_envvar_prefix='BEAR')

#     # pvp_values = ['pd', 'pl']
#     # pvp_values = ["both"]
#     # # num_rows_values = ['1000000', '5000000', '10000000']
#     # num_rows_values = ["100000000"]
#     # num_int_cols_values = ["5"]
#     # num_float_cols_values = ["5"]
#     # # num_int_cols_values = ['10']
#     # # num_float_cols_values = ['10']

#     # # RUN FOR WRITING PARQUETs
#     # for pvp in pvp_values:
#     #     for num_rows in num_rows_values:
#     #         for num_int_cols in num_int_cols_values:
#     #             for num_float_cols in num_float_cols_values:
#     #                 run_writing(pvp, num_rows, num_int_cols, num_float_cols)

#     # # RUN FOR READING DATASETS
#     # datasets = butils.find_files_with_prefix('./benchmarks/datasets/', 'dataset_')
#     # for pvp in pvp_values:
#     #     for dataset in datasets:
#     #         run_reading(pvp, dataset)

#     # # RUN FOR FILTERING
#     # datasets = butils.find_files_with_prefix('./benchmarks/datasets/', 'dataset_')
#     # for pvp in pvp_values:
#     #     for dataset in datasets:
#     #         run_filtering(pvp, dataset)

#     # # RUN FOR JOININGS
#     # datasets = butils.find_files_with_prefix('./benchmarks/datasets/', 'dataset_')
#     # type_joins = ['left', 'inner', 'outer']
#     # join_ons = ['country', 'country_code']
#     # for pvp in pvp_values:
#     #     for dataset in datasets:
#     #         for type_join in type_joins:
#     #             for join_on in join_ons:
#     #                 run_joinings(pvp, dataset, type_join, join_on)

#     # # RUN FOR AGGREGATING
#     # datasets = butils.find_files_with_prefix('./benchmarks/datasets/', 'dataset_')
#     # for pvp in pvp_values:
#     #     for dataset in datasets:
#     #         run_aggregating(pvp, dataset)
