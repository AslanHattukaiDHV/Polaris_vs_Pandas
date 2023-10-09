import time
import psutil

def measure_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Record the start time
        result = func(*args, **kwargs)  # Call the decorated function
        end_time = time.time()  # Record the end time

        runtime = end_time - start_time  # Calculate the runtime
        print(f"The function {func.__name__} took {runtime} seconds to run.")

        return result

    return wrapper


## Actually it looks like we should run this in seperate files/functions to get the most accurate results... 
def measure_cpu(func):
    def wrapper(*args, **kwargs):
        cpu_usage_start = psutil.cpu_percent()
        result = func(*args, **kwargs)  # Call the decorated function
        cpu_usage_end = psutil.cpu_percent()
        cpu_usage = cpu_usage_end - cpu_usage_start
        print(f"The function {func.__name__} took {cpu_usage} cpu percentage to run.")

        return result
    
    return wrapper
        


def measure_memory_usage(func):
    def wrapper(*args, **kwargs):
        memory_usage_start = psutil.Process().memory_percent()
        result = func(*args, **kwargs)
        memory_usage_end = psutil.Process().memory_percent()
        memory_usage = memory_usage_end - memory_usage_start
        print(f"The function {func.__name__} took {memory_usage} memory % to run.")

        return result
    
    return wrapper
        