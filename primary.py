import grpc
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import threading
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
import time

class PrimaryServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self):
        """
        Initialize the Primary server servicer.
        
        Sets up:
        - Local storage for records (in-memory and file-based)
        - Connection to the backup server
        - Heartbeat mechanism
        """
        super().__init__()
        self.records_dict = {}  # Dictionary to store records
        self.records = "primary.txt"  # File to store records
        
        # Create persistent channel and stub for backup server communication
        self.backup_channel = grpc.insecure_channel("localhost:50052")
        self.backup_stub = replication_pb2_grpc.SequenceStub(self.backup_channel)
        
        # Initialize heartbeat connection
        self.heartbeat_channel = grpc.insecure_channel('localhost:50053')
        self.heartbeat_stub = heartbeat_service_pb2_grpc.ViewServiceStub(self.heartbeat_channel)
        
        # Load existing records
        self.populate_records()

    def Write(self, request, context):
        """
        Handle write requests from clients.
        
        This method:
        1. Receives a key-value pair from the client
        2. Forwards the write to the backup server
        3. Only commits locally if the backup acknowledges
        4. Returns acknowledgment to the client
        
        Args:
            request: The client's write request containing key and value
            context: The gRPC context
            
        Returns:
            WriteResponse with acknowledgment status
        """
        self.populate_records()
        key = request.key
        value = request.value
        print("Write request received from client")

        try:
            # Forward write request to backup server first (write-ahead approach)
            print("Sending write request to backup")
            write_request = replication_pb2.WriteRequest(key=key, value=value)
            response = self.backup_stub.Write(write_request)
            print(f"Backup response: {response.ack}")
            
            if response.ack == "ack":
                # Only commit locally if backup acknowledges
                print(f"ack received from backup - storing values: {key} {value}")
                self.records_dict[key] = value
                self.write_to_file()
                return replication_pb2.WriteResponse(ack="ack")
            else:
                # Handle unexpected response from backup
                print(f"Unexpected response from backup: {response.message}")
                context.set_code(grpc.StatusCode.DATA_LOSS)
                context.set_details("Backup write failed please try again")
                return replication_pb2.WriteResponse(ack="error")
        except grpc.RpcError as e:
            # Handle communication errors with backup
            print(f"An error occurred: {e.details()}")
            return replication_pb2.WriteResponse(ack="error")

    def __del__(self):
        """
        Clean up resources when the servicer is destroyed.
        """
        # Close the gRPC channels
        self.backup_channel.close()
        self.heartbeat_channel.close()

    def write_to_file(self):
        """
        Write the in-memory records to the persistent storage file.
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
            # Create the file if it doesn't exist
            with open(self.records, "w") as f:
                pass
                
    def Heartbeat(self):
        """
        Send periodic heartbeats to the view service.
        
        This method runs in a separate thread and:
        - Connects to the view service
        - Sends heartbeat messages at regular intervals
        - Identifies itself as the "primary" server
        """
        while True:
            try:
                # Create and send heartbeat request
                heartbeat_request = heartbeat_service_pb2.HeartbeatRequest(service_identifier="primary")
                self.heartbeat_stub.Heartbeat(heartbeat_request)
                time.sleep(5)  # Send heartbeat every 5 seconds
            except Exception as e:
                print(f"Error sending heartbeats: {e}")
            

def serve():
    """
    Start the primary server and set up the heartbeat mechanism.
    
    This function:
    1. Creates a gRPC server
    2. Registers the PrimaryServicer
    3. Starts the heartbeat thread
    4. Starts the server on port 50051
    """
    # Create a gRPC server with 10 worker threads
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = PrimaryServicer()
    
    # Register our servicer with the server
    replication_pb2_grpc.add_SequenceServicer_to_server(servicer, server)

    # Start the heartbeat thread BEFORE starting the server
    # This ensures heartbeats are ready when the server starts accepting requests
    heartbeat_thread = threading.Thread(target=servicer.Heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # Listen on port 50051
    server.add_insecure_port('[::]:50051')
    
    # Start the server
    server.start()
    print("Primary Server started on port 50051")
    
    # Keep the server running until terminated
    server.wait_for_termination()

if __name__ == "__main__":
    serve()


