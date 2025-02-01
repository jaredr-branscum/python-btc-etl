import time
import os
import sys
from sqlalchemy import text

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the ETL application components
from btc_etl import (
    initialize_database,
    process_existing_files,
    TABLE_NAME,
    engine,
    get_redis_connection,
    MAX_THREADS,
)

# Override the DATA_DIRECTORY to point to the root-level dataset-test folder
DATA_DIRECTORY = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dataset-test"))

def setup_benchmark_environment():
    # Reset Redis cache
    r = get_redis_connection()
    r.delete("processed_files")  # Clear the set of processed files

    # Reset database
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE_NAME}"))

def run_benchmark(multithreaded):
    # Set the environment variable for multithreading
    os.environ["ENABLE_MULTITHREADING"] = str(multithreaded)
    print(f"ENABLE_MULTITHREADING is set to: {os.getenv('ENABLE_MULTITHREADING')}")  # Debug log

    # Initialize the database
    initialize_database()

    # Measure the time taken to process files
    start_time = time.time()
    process_existing_files()
    end_time = time.time()

    # Return the elapsed time
    return end_time - start_time

def calculate_percentage_increase(non_multithreaded_time, multithreaded_time):
    speedup = non_multithreaded_time / multithreaded_time
    percentage_increase = (speedup - 1) * 100
    return percentage_increase

def main():
    # Set up the benchmark environment
    setup_benchmark_environment()

    # Run the benchmark in multithreaded mode
    multithreaded_time = run_benchmark(multithreaded=True)
    print(f"Multithreaded mode took {multithreaded_time:.2f} seconds.")

    # Reset the environment for the next run
    setup_benchmark_environment()

    # Run the benchmark in non-multithreaded mode
    non_multithreaded_time = run_benchmark(multithreaded=False)
    print(f"Non-multithreaded mode took {non_multithreaded_time:.2f} seconds.")

    # Print the performance comparison
    print("\nPerformance Comparison:")
    print(f"Multithreaded: {multithreaded_time:.2f} seconds (using {MAX_THREADS} threads)")
    print(f"Non-multithreaded: {non_multithreaded_time:.2f} seconds")
    print(f"Speedup: {non_multithreaded_time / multithreaded_time:.2f}x")
    percentage_increase = calculate_percentage_increase(non_multithreaded_time, multithreaded_time)
    print(f"The multi-threaded implementation is {percentage_increase:.2f}% faster.")

if __name__ == "__main__":
    main()