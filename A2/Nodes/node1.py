import os
import json
import grpc
import random
import asyncio
import raftkv_pb2
import raftkv_pb2_grpc
from concurrent import futures

node_addresses = {
    'node_1': 'localhost:50051',
    'node_2': 'localhost:50052',
    'node_3': 'localhost:50053',
}

class RaftNode:
    def __init__(self, nodeID, partitions):
        self.node_id = nodeID
        self.partitions = partitions
        self.state = 2                  # 0 = Leader, 1 = Candidate, 2 = Follower
        self.current_term = 0
        self.voted_for = None
        self.log = []                   # List of log entries; each entry is a dictionary
        self.data = {}
        self.commit_index = 0
        self.election_timeout = random.uniform(5, 10)
        self.heartbeat_interval = 1.0       # 1 second
        self.lease_expiration_time = 10.0   # 10 second
        self.lease_on = False
        self.leader_id = None
        self.initialize_storage()

    def initialize_storage(self):
        # Load or initialize persistent state (term, votedFor, log)
        directory = f"logs_node_{self.node_id}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.log_file_path = os.path.join(directory, "logs.txt")
        self.metadata_file_path = os.path.join(directory, "metadata.txt")
        self.dump_file_path = os.path.join(directory, "dump.txt")
        self.data_file_path = os.path.join(directory, "data.txt")
        # Try to load the log from disk
        try:
            with open(self.log_file_path, 'r') as log_file:
                self.log = json.load(log_file)
        except FileNotFoundError:
            self.log = []  # Initialize log as empty if file does not exist
        
        # Try to load metadata from disk
        try:
            with open(self.metadata_file_path, 'r') as metadata_file:
                metadata = json.load(metadata_file)
                self.current_term = metadata['current_term']
                self.voted_for = metadata.get('voted_for')  # Might be None if not set
                self.commit_index = metadata.get('commitLength')
        except FileNotFoundError:
            # Initialize metadata if file does not exist
            self.current_term = 0
            self.voted_for = None
            self.save_metadata()

        # Try loading the data
        try:
            with open(self.data_file_path, 'r') as f:
                self.data = json.load(f)
        except FileNotFoundError:
            self.data = {}

    def dump_operation(self, lines):
        try:
            with open(self.dump_file_path, 'a') as f:
                for line in lines:
                    # Ensure each line ends with a newline character
                    if not line.endswith('\n'):
                        line += '\n'
                    f.write(line)
        except:
            print("dumping operation ran into some error")

    def save_metadata(self):
        # Save current term and voted_for to disk
        metadata = {
            'commitLength': self.commit_index,
            'current_term': self.current_term,
            'voted_for': self.voted_for
        }
        with open(self.metadata_file_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file)

    def commit(self):
        try:
            with open(self.data_file_path, 'w') as data_file:
                json.dump(self.data, data_file)
        except:
            print("Data commit unsuccessful")
        try:
            with open(self.log_file_path, 'w') as log_file:
                json.dump(self.log, log_file)
        except:
            print("Logs commit unsuccessful")
        try:
            self.save_metadata()
        except:
            print("Metadata commit unsuccessful")

    def ServeClient(self, request, context):
        if self.state != 0:  # If not the leader (0 is leader, 1 is candidate, 2 is follower)
            leader_address = self.node_addresses.get(self.leader_id, "")
            return raftkv_pb2.ServeClientReply(success=False, leaderID=self.leader_id, data=leader_address)

        # Handle the request if this node is the leader
        operation, key, value = request.request.split(maxsplit=2)
        
        if operation.upper() == "SET":
            # Logic for handling SET operation
            self.handle_set_operation(key, value)
            return raftkv_pb2.ServeClientReply(success=True, leaderID=self.node_id, data="SET operation successful")
        elif operation.upper() == "GET":
            # Logic for handling GET operation
            value = self.handle_get_operation(key)
            return raftkv_pb2.ServeClientReply(success=True, leaderID=self.node_id, data=value)
        else:
            return raftkv_pb2.ServeClientReply(success=False, leaderID=self.node_id, data="Invalid operation")
    
    def handle_set_operation(self, key, value):
        # Append the operation to the log
        entry = {"term": self.current_term, "Operation": "SET", "key": key, "value": value}
        self.log.append(entry)
        # Replicate the log entry to followers (not fully implemented here)
        # Commit the log entry (not fully implemented here)
        # Update the data store
        self.data[key] = value
        self.commit()  # Commit changes to persistent storage

    def handle_get_operation(self, key):
        # Retrieve the value from the data store
        return self.data.get(key, "")

    
async def serve(node_id, node_addresses):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_node = RaftNode(node_id=node_id, node_addresses=node_addresses)
    raftkv_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    server.add_insecure_port(node_addresses[node_id])
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # Example node_addresses format: {'node_1': 'localhost:50051', 'node_2': 'localhost:50052'}
    node_id = 'node_1'
    # asyncio.run(serve(node_id, node_addresses))
    raft_node = RaftNode(node_id, node_addresses)