import grpc
from concurrent import futures

import threading
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
import time
from google.protobuf import empty_pb2

class HeartbeatServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):
    """
    View Service that monitors the health of primary and backup servers.
    
    This service:
    - Receives heartbeats from servers
    - Tracks which servers are alive
    - Logs server status changes
    - Detects when servers might be down
    """
    def __init__(self):
        """
        Initialize the Heartbeat servicer.
        
        Sets up:
        - Dictionary to track live servers
        - Background thread for monitoring server health
        """
        super().__init__()
        # Dictionary to store server IDs and their last heartbeat timestamps
        self.live_servers = {}
        
        # Start a background thread to monitor server health
        self.monitor_thread = threading.Thread(target=self.monitor_servers, daemon=True)
        self.monitor_thread.start()
        
    def Heartbeat(self, request, context):
        """
        Handle heartbeat requests from servers.
        
        This method:
        1. Extracts the server ID from the request
        2. Records the current time as the last heartbeat time
        3. Logs the heartbeat
        
        Args:
            request: The heartbeat request containing the server identifier
            context: The gRPC context
            
        Returns:
            Empty response
        """
        server_id = request.service_identifier
        current_time = time.time()  # Get current timestamp
        
        # Update the last heartbeat time for this server
        self.live_servers[server_id] = current_time
        
        # Log the heartbeat (type 1 = alive)
        self.heartbeat_log(id=server_id, timestamp=current_time, type=1)
        
        # Print status to console
        print(f"{server_id} is alive. Latest heartbeat received at {current_time}")
        
        # Return empty response
        return empty_pb2.Empty()

    def heartbeat_log(self, id, timestamp, type=1):
        """
        Log heartbeat events to a file.
        
        Args:
            id (str): The server identifier
            timestamp (float): The time of the event
            type (int): Event type - 1 for alive, 0 for potentially down
        """
        if type == 1:  # If it's a live update
            line = f" {id} is alive. Latest heartbeat received at {timestamp}\n"
        else:  # If server might be down
            line = f"{id} might be down. Latest heartbeat received at {timestamp}\n"
            
        # Append the log entry to the heartbeat log file
        with open("heartbeat.txt", "a") as file:
            file.write(line)
            
    def monitor_servers(self):
        """
        Background thread that monitors server health.
        
        This method:
        - Runs continuously in the background
        - Checks the last heartbeat time of each server
        - Marks servers as potentially down if no heartbeat received within timeout
        - Removes down servers from the live servers list
        """
        while True:
            current_time = time.time()  # Get current timestamp
            
            # Check each server's last heartbeat time
            for server_id, last_heartbeat in list(self.live_servers.items()):
                # If no heartbeat received in the last 15 seconds
                if current_time - last_heartbeat > 15:  # Timeout threshold
                    # Log that the server might be down (type 0 = down)
                    self.heartbeat_log(id=server_id, timestamp=current_time, type=0)
                    
                    # Print status to console
                    print(f"{server_id} might be down. Latest heartbeat received at {current_time}")
                    
                    # Remove the server from the live servers dictionary
                    del self.live_servers[server_id]
                    
            # Sleep for 5 seconds before checking again
            time.sleep(5)
       
def serve():
    """
    Start the heartbeat service.
    
    This function:
    1. Creates a gRPC server
    2. Registers the HeartbeatServicer
    3. Starts the server on port 50053
    """
    # Create a gRPC server with 10 worker threads
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Create and register our servicer with the server
    servicer = HeartbeatServicer()
    heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(servicer=servicer, server=server)
    
    # Listen on port 50053
    server.add_insecure_port('[::]:50053')
    
    # Start the server
    server.start()
    print("Heartbeat Server started on port 50053")
    
    # Keep the server running until terminated
    server.wait_for_termination()

if __name__ == "__main__":
    # Entry point of the application
    serve()



