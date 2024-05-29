import csv
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

def map_function(row):
    """Map function that returns a tuple of passenger IDs and flight counts."""
    passenger_id = row[0].strip()
    return passenger_id, 1

def map_csv(input_file_path, num_threads):
    """Multi-threaded Map function that reads a CSV file and applies map_function."""
    #A defaultdict object is created to store the number of flights for each passenger, with an initial value of zero.
    map_results = defaultdict(int)
    with open(input_file_path, 'r') as file:
        reader = csv.reader(file)
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit map_function to the thread pool for execution.
            futures = [executor.submit(map_function, row) for row in reader]
            for future in futures:
                passenger_id, count = future.result()
                map_results[passenger_id] += count  # Accumulate the number of flights per passenger
    # Returns a dictionary containing the sum of the passenger ID and the number of flights.
    return map_results