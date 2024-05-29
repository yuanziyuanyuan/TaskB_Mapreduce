from map import map_csv
from reduce import partition, reduce_partitions

def main():
    input_file_path = "data/AComp_Passenger_data_no_error.csv" #Specify the input file path
    num_threads = 4  # Setting the number of threads
    num_partitions = 5  # Setting the number of partitions

    # Map stage
    # Calling the map_csv function performs the Map phase and stores the result in the map_result variable.
    map_result = map_csv(input_file_path, num_threads)
    print("Map result:", map_result)

    # Shuffle stage
    # Calling the partition function performs a Shuffle phase, partitioning the Map results and storing them in the partitions variable.
    partitions = partition(map_result, num_partitions)
    print("Partitions result:", partitions)

    # Reduce stage
    # The reduce_partitions function is called to perform the Reduce phase and store a sorted list of all passenger data in the all_passengers_data variable.
    all_passengers_data = reduce_partitions(partitions, num_threads)

    # Find the passenger with the highest number of flights
    if all_passengers_data:
        max_flights = all_passengers_data[0][1]
        # Find a list of all passenger IDs with a number of flights equal to the maximum number of flights.
        passengers_with_max_flights = [item[0] for item in all_passengers_data if item[1] == max_flights]

        # Converting a list of passenger IDs to strings
        passengers_with_max_flights_str = ", ".join(str(passenger_id) for passenger_id in passengers_with_max_flights)

        # Prints out passenger information based on number of passengers and number of flights.
        if len(passengers_with_max_flights) > 1:
            print(f"The passengers with IDs '{passengers_with_max_flights_str}' have the highest number of flights, totaling {max_flights} flights.")
        else:
            print(f"The passenger with ID '{passengers_with_max_flights_str}' has the highest number of flights, totaling {max_flights} flights.")

if __name__ == "__main__":
    main()