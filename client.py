import replication_pb2
import replication_pb2_grpc
import grpc

# Client application for a distributed key-value store
# This client connects to a primary server, sends write requests,
# and maintains a local copy of the data for redundancy

def run():
    """
    Main function that runs the client application.
    
    This function:
    1. Loads existing records from the local file
    2. Establishes a connection to the primary server
    3. Handles user input for write operations
    4. Sends write requests to the primary server
    5. Updates local storage upon successful writes
    6. Continues until the user chooses to quit
    """
    running = True
    # Load existing records from file into memory
    records_dict = populate_records()
    
    # Connect to primary once outside the loop for efficiency
    with grpc.insecure_channel("localhost:50051") as channel:
        # Create a stub for making RPC calls to the primary server
        stub = replication_pb2_grpc.SequenceStub(channel)
        
        while running:
            # Get user input for operation selection
            select = input("Enter 1 to write and q to quit: ")
            if select == 'q':
                # Exit the loop if user chooses to quit
                running = False
                break
            elif select == '1':
                # Get key and value from user for write operation
                key = input("Please enter key: ")
                value = input("Please enter value: ")
                try: 
                    # Create a write request with the provided key-value pair
                    request = replication_pb2.WriteRequest(key=key, value=value)
                    # Send the write request to the primary server
                    response = stub.Write(request)
                    print(f"Primary response: {response.ack}")
                    
                    # If the primary acknowledges the write, update local storage
                    if response.ack == "ack":
                        print(f"ack received from primary- storing values: {key} {value}")
                        # Update in-memory dictionary
                        records_dict[key] = value 
                        # Persist changes to local file
                        write_to_file(records_dict)
                         
                except grpc.RpcError as e:
                    # Handle any gRPC errors that occur during the request
                    print(f"An error occurred: {e.details()}")
            else:
                # Handle invalid input
                print("please enter a valid input value, 1 for a write and q to quit")

def populate_records(file="client.txt"):
    """
    Loads existing records from a file into a dictionary.
    
    Args:
        file (str): Path to the file containing key-value records.
                    Defaults to "client.txt".
    
    Returns:
        dict: Dictionary containing key-value pairs loaded from the file.
    
    Note:
        - Creates the file if it doesn't exist
        - Each line in the file should be in the format "key value"
        - Skips empty lines and ensures both key and value are present
    """
    records_dict = {}
    try:
        # Attempt to open and read the file
        with open(file, "r") as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    split_line = line.split(maxsplit=1)
                    if len(split_line) >= 2:  # Make sure we have both key and value
                        key = split_line[0]
                        value = split_line[1]
                        records_dict[key] = value
    except FileNotFoundError:
        # Create the file if it doesn't exist
        with open(file, "w") as f:
            pass
    return records_dict

def write_to_file(records, file="client.txt"):
    """
    Writes the records dictionary to a file.
    
    Args:
        records (dict): Dictionary containing key-value pairs to write.
        file (str): Path to the file where records will be written.
                    Defaults to "client.txt".
    
    Note:
        - Overwrites the existing file
        - Each key-value pair is written as "key value" on a new line
    """
    with open(file, "w") as f:
        for key, value in records.items():
            record = f"{key} {value}\n"
            f.write(record)


if __name__ == "__main__":
    # Entry point of the application
    run()


        


