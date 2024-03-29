import os
import json
import time
import grpc.aio as grpc
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
        self.acks = {}                    # dictionary to save the last index of every node
        for node_id, address in node_addresses.items():
            self.acks[node_id] = 0

        self.state = "follower"           # follower, candidate, leader
        self.current_term = 0
        self.voted_for = None
        self.commit_log = []
        self.log = []                   # List of log entries; each entry is a dictionary
        self.data = {}
        self.commit_index = 0
        self.election_timeout = random.uniform(5, 10)
        self.heartbeat_interval = 1.0       # 1 second
        self.heartbeat_received = False
        self.lease_with_others = 0
        self.lease_expiration_time = None 
        self.lease_duration = 5.0           # 5 second
        self.leader_id = None
        self.lock = asyncio.Lock()
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
                self.acks = metadata.get('acks')
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
        
        temp_text = f"Node {self.node_id} started"
        self.dump_operation(temp_text)
        print(temp_text)
        self.save_metadata

    def dump_operation(self, line):
        try:
            with open(self.dump_file_path, 'a') as f:
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
            'voted_for': self.voted_for,
            'acks': self.acks
        }
        with open(self.metadata_file_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file)

    async def save_state(self):
        async with self.lock:
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
    
    def get_last_log_index(self):
        return len(self.commit_index) if self.log else 0

    def get_last_log_term(self):
        return self.log[self.commit_index]['term'] if self.log else 0

    def calculate_lease_duration_remaining(self):
        if not self.lease_expiration_time:
            return 0
        now = time.time()
        return max(0, self.lease_expiration_time - now)
        
    async def run(self):
        # Start the election timer
        print(f"Node {self.node_id} started, election timer started with {self.election_timeout} seconds")
        asyncio.create_task(self.election_timer())
        
        # Additional logic for starting the server, handling RPCs, etc.

    async def become_leader(self):
        self.state = 'leader'  # Become the leader
        self.leader_id = self.node_id
        self.lease_expiration_time = time.time() + self.lease_duration
        asyncio.create_task(self.lease_timer())
        temp_text = f"Node {self.node_id} became the leader for term {self.current_term}"
        print(temp_text)
        self.dump_operation(temp_text)
        # Start sending heartbeats
        asyncio.create_task(self.send_heartbeats())
    
    async def lease_timer(self):
        if self.lease_with_others:
            temp_text = "New Leader waiting for Old Leader Lease to timeout."
            print(temp_text)
            self.dump_operation(temp_text)
            await asyncio.sleep(self.lease_with_others)
        
        self.lease_with_others = 0

        if self.state == 'leader' and not self.lease_with_other:
            while self.state == 'leader' and self.lease_expiration_time and time.time() < self.lease_expiration_time:
                await asyncio.sleep(0.5)  # Check lease status every 0.5 seconds
            if self.state == 'leader':  # If still the leader but lease expired
                temp_text = f"Leader {self.node_id} lease renewal failed. Stepping Down."
                self.dump_operation(temp_text)
                print(temp_text)
                self.state = 'follower'  # Become a follower
                self.voted_for = None
                self.leader_id = None
    
    async def election_timer(self):
        while True:
            await asyncio.sleep(self.election_timeout)
            if not self.heartbeat_received and self.state != 'leader':
                dump_text = f"Node {self.node_id} election timer timed out, starting election"
                print(dump_text)
                self.dump_operation(dump_text)
                self.start_election()
            self.heartbeat_received = False  # Reset 
    
    async def start_election(self):
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.heartbeat_received = False  # Reset heartbeat received flag
        max_lease = 0
        print(f"Node {self.node_id} starting an election for term {self.current_term}.")
        votes_received = 1  # Vote for itself
        # Async method to send out RequestVote RPCs to all nodes and await responses
        for node_id, address in node_addresses.items():
            if node_id != self.node_id:
                response = await self.request_vote(node_id, address)
                if response.voteGranted:
                    votes_received += 1
                    max_lease = max(max_lease, response.leaseDurationRemaining)

        if votes_received > len(node_addresses) // 2:
            temp_text = f"Node {self.node_id} became the leader for term {self.current_term}."
            print(temp_text)
            self.dump_operation(temp_text)
            self.lease_with_others = max_lease
            await self.become_leader()

    async def send_heartbeats(self):
        while self.state == 'leader' and not self.lease_with_others and self.lease_expiration_time:
            if time.time() >= self.lease_expiration_time:
                temp_text = f"Leader {self.node_id} lease renewal failed. Stepping Down."
                print(temp_text)
                self.dump_operation(temp_text)
                self.state = 'follower'
                self.voted_for = None
                self.leader_id = None
                break  # Exit the loop if the lease has expired
            
            acks_received = 1
            async with self.lock:
                start_ind = self.commit_index+1
                end_ind = len(self.log)-1
            entries = [raftkv_pb2.LogEntry(term=e['term'], key=e['key'], value=e['value']) for e in self.log[start_ind:]]
            request = raftkv_pb2.AppendEntriesRequest(
                term=self.current_term,
                leaderId=self.node_id,
                prevLogIndex= self.get_last_log_index,
                prevLogTerm=self.get_last_log_term,
                entries=entries,
                leaderCommit=self.commit_index,
                leaseDuration=self.lease_duration  # Fixed lease duration
            )

            """
            So, now that you have given the enteries, some will acknowledge, some will fail, some will reject, look into all situations.
            Also, once majority is established, commit new ones (for leader), next heartbeat (followers commit).
            Next, look into file management using global semaphores/mutexes for committing
            Lastly, when a client has sent request, check using index if that entry is commited, if yes then success, else wait for 0.5 seconds.
            """

            for node_id, address in node_addresses.items():
                if node_id == self.node_id:
                    continue  # Skip self
                response = await self.send_append_entries(node_id, address, request)
                if response and response.success: 
                    acks_received += 1
                    self.acks[node_id] = self.commit_index
                elif response and not response.success:
                    if response.term > self.current_term:
                        self.current_term = request.term
                        self.voted_for = None
                        self.state = "follower"
                        break

                    if self.acks[node_id] > 0:
                        self.acks[node_id] = self.acks[node_id] - 1
                    elif self.commit_index > 0:
                        self.acks[node_id] = self.commit_index - 1
                    else:
                        self.acks[node_id] = 0

            # If majority is achieved, update commit index and commit new entries
            if acks_received > len(node_addresses) // 2:
                new_commit_index = end_ind  # Assume all new entries are now committed
                if new_commit_index > self.commit_index:
                    self.commit_index = new_commit_index
                    self.commit()  # Commit changes to the log and data
                    if self.lease_expiration_time and time.time() + 2 >= self.lease_expiration_time:
                        self.lease_expiration_time = time.time() + self.lease_duration
                        print("Lease renewed.")
                        temp_text = f"Leader {self.node_id} sending heartbeat & Renewing Lease"
                        self.dump_operation(temp_text)
                    else:
                        print("Lease renewal failed")

            await asyncio.sleep(self.heartbeat_interval)

    async def AppendEntries(self, request, context):
        # Check if the term is current
        if request.term < self.current_term:
            return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=False)
        
        # Reset the election timer on receiving heartbeat/AppendEntries
        self.heartbeat_received = True
        self.leader_id = request.leaderId

        # Log consistency check
        if request.prevLogIndex >= self.get_last_log_index or (request.prevLogIndex >= 0 and self.get_last_log_term != request.prevLogTerm):
            return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=False)
        
        # Update the log as per entries received
        # This is simplified; actual implementation should handle log consistency checks
        if request.entries:
            self.log.extend([{"term": e.term, "key": e.key, "value": e.value} for e in request.entries])
            self.commit_index = min(request.leaderCommit, len(self.log) - 1)
        
        # Commit changes to persistent storage
        
        return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=True)
    
    def RequestVote(self, request, context):
        # Calculate the remaining lease duration
        lease_duration_remaining = self.calculate_lease_duration_remaining()

        if request.term < self.current_term:
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False, leaseDurationRemaining=lease_duration_remaining)

        # Update current term and reset votedFor if term is outdated
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = "follower"

        if (self.voted_for is None or self.voted_for == request.candidateId) and self.is_log_up_to_date(request.lastLogIndex, request.lastLogTerm):
            self.voted_for = request.candidateId
            self.save_metadata()
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=True, leaseDurationRemaining=lease_duration_remaining)
        else:
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False, leaseDurationRemaining=lease_duration_remaining)
    
    async def request_vote(self, node_id, address):
        if self.state == 'candidate':
            async with grpc.aio.insecure_channel(address) as channel:
                stub = raftkv_pb2_grpc.RaftStub(channel)
                # Construct the RequestVoteRequest message
                request = raftkv_pb2.RequestVoteRequest(
                    term=self.current_term,
                    candidateId=int(self.node_id),
                    lastLogIndex=self.get_last_log_index(),
                    lastLogTerm=self.get_last_log_term(),
                    leaseDurationRemaining=self.calculate_lease_duration_remaining()
                )
                try:
                    # Send the request and await the response
                    response = await stub.RequestVote(request, timeout=5)  # Consider a suitable timeout
                    return response
                except grpc.aio.AioRpcError as e:
                    print(f"RPC failed to node {node_id}: {e}")
                    # Handle the error appropriately. Perhaps return None or a default response object.
                    return None
        else:
            print("Not a candidate, back to follower")
            self.state = 'follower'
            self.voted_for = None
            self.leader_id = None
            return None

    
    def is_log_up_to_date(self, candidate_last_log_index, candidate_last_log_term):
        # If the log is empty, treat the term and index as 0
        last_log_index = self.get_last_log_index()
        last_log_term = self.get_last_log_term()
        
        # First, compare the term of the last log entry
        if last_log_term < candidate_last_log_term:
            return True
        elif last_log_term > candidate_last_log_term:
            return False
        
        # If the terms are equal, compare the indices of the last log entry
        return last_log_index <= candidate_last_log_index


    def ServeClient(self, request, context):
        if self.state != 'leader':  
            leader_address = self.node_addresses.get(self.leader_id, "")
            return raftkv_pb2.ServeClientReply(success=False, leaderID=self.leader_id, data=leader_address)
        
        # Handle the request if this node is the leader
        operation, key, value = request.request.split(maxsplit=2)
        
        if operation.upper() == "SET":
            # Logic for handling SET operation
            self.handle_set_operation(key, value)

            #################################################################################################################################
            # wait for the enteries to get commited in the leader then give success response, else fail
            #################################################################################################################################

            return raftkv_pb2.ServeClientReply(success=True, leaderID=self.node_id, data="SET operation successful")
        elif operation.upper() == "GET":
            # Logic for handling GET operation
            value = self.handle_get_operation(key)
            return raftkv_pb2.ServeClientReply(success=True, leaderID=self.node_id, data=value)
        else:
            return raftkv_pb2.ServeClientReply(success=False, leaderID=self.node_id, data="Invalid operation")
    
    async def handle_set_operation(self, key, value):
        if self.state == 'leader' and not self.lease_with_others and self.lease_expiration_time and time.time() < self.lease_expiration_time:
            # Only the leader with a valid lease can handle SET operations
            entry = {"term": self.current_term, "Operation": "SET", "key": key, "value": value}
            self.log.append(entry)

            await self.replicate_log_entries()

            # Update the data store
            self.data[key] = value
            self.commit()  # Commit changes to persistent storage
            dump_text = f"Node {self.node_id} {self.state} committed the entry {entry} to the state machine"
            self.dump_operation(dump_text)
        else:
            print("Not the leader or lease expired; cannot handle SET operation.")

    def handle_get_operation(self, key):
        if self.state == 'leader' and not self.lease_with_others and self.lease_expiration_time and time.time() < self.lease_expiration_time:
            # Only the leader with a valid lease can handle GET operations with guaranteed consistency
            return self.data.get(key, "")
        else:
            # Follower nodes handling or leader without a valid lease
            # Depending on consistency requirements, consider redirecting to leader or serving stale data
            return "Redirect to leader or operation not allowed"

    
async def serve(node_id, node_addresses):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_node = RaftNode(node_id, node_addresses)
    raftkv_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    server.add_insecure_port(node_addresses[node_id])
    await server.start()
    try:
        await raft_node.run()  # Start the Raft node's tasks
        await server.wait_for_termination()
    except KeyboardInterrupt:
        # Attempt a graceful shutdown
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await server.stop(0)  # Await the server shutdown here
        print("Server and all tasks have been shut down gracefully.")

if __name__ == '__main__':
    # Example node_addresses format: {'node_1': 'localhost:50051', 'node_2': 'localhost:50052'}
    node_id = 'node_1'
    asyncio.run(serve(node_id, node_addresses))