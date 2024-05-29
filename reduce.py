import os
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

def partition(map_result, num_partitions):
    """Shuffle phase, which partitions the Map results according to the partitioning rules."""
    #A dictionary partitions is created where the key is the number of the partition and the value is an empty defaultdict object that stores the passenger ID and number of flights for each partition.
    partitions = {i: defaultdict(int) for i in range(num_partitions)}
    for passenger_id, count in map_result.items():
        #Calculate the hash value of the passenger ID and use it to perform a remainder operation on num_partitions to get the partition number.
        partition_key = hash(passenger_id) % num_partitions
        partitions[partition_key][passenger_id] += count

    return partitions


def reduce_function(partition_data, partition_index):
    """Reduce function that writes all passenger IDs in the partition and their flight counts to a file."""
    if not os.path.exists('result'):
        os.makedirs('result')

    partition_file_path = os.path.join('result', f'partition_{partition_index}.txt')

    with open(partition_file_path, "w") as f:
        for passenger_id, count in sorted(partition_data.items(), key=lambda item: item[1], reverse=True):
            f.write(f"{passenger_id},{count}\n")

    # Returns a sorted list
    return sorted(partition_data.items(), key=lambda item: item[1], reverse=True)


def reduce_partitions(partitions, num_threads):
    """Multi-threaded Reduce function that performs a Reduce operation on the data in each partition and collects all passenger data."""
    all_passengers_data = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(reduce_function, partition, i): i for i, partition in partitions.items()}
        for future in futures:
            partition_index = futures[future]
            passengers_data = future.result()
            all_passengers_data.extend([(passenger_id, count) for passenger_id, count in passengers_data])  # 确保是列表的列表

    # Sort all passengers by number of flights in descending order
    all_passengers_data.sort(key=lambda item: item[1], reverse=True)

    # Construct the path to the final sort result file
    final_sorted_file_path = os.path.join('result', 'final_sorted_results.txt')

    # Write the final sort result to a txt file in the result folder.
    with open(final_sorted_file_path, "w") as f_final:
        for passenger_id, count in all_passengers_data:
            f_final.write(f"{passenger_id},{count}\n")

    return all_passengers_data  # Returns a sorted list of all passenger data