import grpc
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import threading
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
import time

class BackupServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self):
        """
        Initialize the Backup server servicer.
        
        Sets up:
        - Local storage for records (in-memory and file-based)
        - Heartbeat mechanism for communicating with the view service
        """
        super().__init__()
        self.records_dict = {}  # Dictionary to store records
        self.records = "backup.txt"  # File to store records
        
        # Initialize heartbeat connection
        self.heartbeat_channel = grpc.insecure_channel('localhost:50053')
        self.heartbeat_stub = heartbeat_service_pb2_grpc.ViewServiceStub(self.heartbeat_channel)
        
        # Load existing records
        self.populate_records()
        

    def Write(self, request, context):
        """
        Handle write requests from the primary server.
        
        This method:
        1. Receives a key-value pair from the primary
        2. Stores it in the local dictionary and file
        3. Returns acknowledgment to the primary
        
        Args:
            request: The primary's write request containing key and value
            context: The gRPC context
            
        Returns:
            WriteResponse with acknowledgment status
        """
        key = request.key
        value = request.value

        # Log the received request
        print("Write request received from primary")

        # Ensure we have the latest data
        self.populate_records()
        
        # Write to dictionary and file 
        print("Storing data")
        self.records_dict[key] = value  # Update in-memory dictionary
        self.write_to_file()  # Persist to file
        
        # Acknowledge the write
        return replication_pb2.WriteResponse(ack="ack")

    def write_to_file(self):
        """
        Write the in-memory records to the persistent storage file.
        
        This ensures data durability even if the server crashes.
        """
        with open(self.records, "w") as f:
            for key, value in self.records_dict.items():
                record = f"{key} {value}\n"
                f.write(record)

    def populate_records(self):
        """
        Load existing records from file into memory.
        
        This method:
        - Reads the records file line by line
        - Parses key-value pairs
        - Populates the in-memory dictionary
        - Creates the file if it doesn't exist
        """
        try:
            with open(self.records, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:  # Skip empty lines
                        split_line = line.split(maxsplit=1)
                        if len(split_line) >= 2:  # Make sure we have both key and value
                            key = split_line[0]
                            value = split_line[1]
                            self.records_dict[key] = value
        except FileNotFoundError:
            # Create the file if it does not exist
            with open(self.records, "w") as f:
                pass

    def Heartbeat(self):
        """
        Send periodic heartbeats to the view service.
        
        This method runs in a separate thread and:
        - Connects to the view service
        - Sends heartbeat messages at regular intervals
        - Identifies itself as the "backup" server
        """
        while True:
            try:
                # Create and send heartbeat request
                heartbeat_request = heartbeat_service_pb2.HeartbeatRequest(service_identifier="backup")
                self.heartbeat_stub.Heartbeat(heartbeat_request)
                time.sleep(5)  # Send heartbeat every 5 seconds
            except Exception as e:
                print(f"Error sending heartbeats: {e}")
    
    def __del__(self):
        """
        Clean up resources when the servicer is destroyed.
        """
        # Close the gRPC channel
        self.heartbeat_channel.close()

def serve():
    """
    Start the backup server and set up the heartbeat mechanism.
    
    This function:
    1. Creates a gRPC server
    2. Registers the BackupServicer
    3. Starts the heartbeat thread
    4. Starts the server on port 50052
    """
    # Create a gRPC server with 10 worker threads
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Create and register our servicer with the server
    servicer = BackupServicer()
    replication_pb2_grpc.add_SequenceServicer_to_server(servicer, server)
    
    # Start the heartbeat thread BEFORE starting the server
    # This ensures heartbeats are ready when the server starts accepting requests
    heartbeat_thread = threading.Thread(target=servicer.Heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # Listen on port 50052
    server.add_insecure_port('[::]:50052')
    
    # Start the server
    server.start()
    print("Backup server started on port 50052")
    
    # Keep the server running until terminated
    server.wait_for_termination()

if __name__ == "__main__":
    serve()


