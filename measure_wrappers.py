import time

def measure_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Record the start time
        result = func(*args, **kwargs)  # Call the decorated function
        end_time = time.time()  # Record the end time

        runtime = end_time - start_time  # Calculate the runtime
        print(f"The function {func.__name__} took {runtime} seconds to run.")

        return result

    return wrapper
