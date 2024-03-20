import threading
import grpc
import random
import time
from concurrent import futures
import raftkv_pb2
import raftkv_pb2_grpc
import json
import os
import asyncio

node_addresses = {
    'node_1': 'localhost:50051',
    'node_2': 'localhost:50052',
    'node_3': 'localhost:50053',
}

class RaftNode(raftkv_pb2_grpc.RaftServicer):
    def __init__(self, node_id, node_addresses):
        self.node_id = node_id
        self.node_addresses = node_addresses  # Dictionary of node_id to address
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.data = {}
        self.log = []  # List of log entries; each entry is a dictionary
        self.commit_index = 0
        self.election_timeout = self.reset_election_timeout()
        self.heartbeat_interval = 1.0       # 1 second
        self.lease_expiration_time = 10.0   # 10 second
        self.leader_id = None
        self.initialize_storage()

    def initialize_storage(self):
        # Load or initialize persistent state (term, votedFor, log)
        directory = f"logs_node_{self.node_id}"
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.log_file_path = os.path.join(directory, "logs.txt")
        self.metadata_file_path = os.path.join(directory, "metadata.txt")
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
        except FileNotFoundError:
            # Initialize metadata if file does not exist
            self.current_term = 0
            self.voted_for = None
            self.save_metadata()

    def save_metadata(self):
        # Save current term and voted_for to disk
        metadata = {
            'current_term': self.current_term,
            'voted_for': self.voted_for
        }
        with open(self.metadata_file_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file)

    def append_entry_to_log(self, entry):
        # Append an entry to the log and save to disk
        self.log.append(entry)
    
    def commit_entry_to_log(self):
        with open(self.log_file_path, 'w') as log_file:
            json.dump(self.log, log_file)

    def RequestVote(self, request, context):
        # Check if term is outdated
        if request.term < self.current_term:
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)

        # Vote if haven't voted for anyone or voted for this candidate, and candidate's log is up-to-date
        if (self.voted_for is None or self.voted_for == request.candidateId) and self.is_candidate_log_up_to_date(request.lastLogIndex, request.lastLogTerm):
            self.voted_for = request.candidateId
            self.current_term = request.term
            # Update persistent state before replying
            self.save_metadata()
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=True)
        else:
            return raftkv_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)

    def is_candidate_log_up_to_date(self, candidate_last_log_index, candidate_last_log_term):
        # If the log is empty, treat the term and index as 0
        last_log_index = 0
        last_log_term = 0
        if self.log:
            last_log_index = len(self.log) - 1
            last_log_term = self.log[-1]['term']
        
        # First, compare the term of the last log entry
        if last_log_term < candidate_last_log_term:
            return True
        elif last_log_term > candidate_last_log_term:
            return False
        
        # If the terms are equal, compare the indices of the last log entry
        return last_log_index <= candidate_last_log_index
    
    async def send_heartbeat(self, node_id, address, request):
        try:
            async with grpc.aio.insecure_channel(address) as channel:
                stub = raftkv_pb2_grpc.RaftStub(channel)
                await stub.AppendEntries(request)
                return True  # Acknowledgment received
        except grpc.aio.AioRpcError as e:
            print(f"Error sending heartbeat to {node_id}: {e}")
            return False  # Acknowledgment failed
    
    async def send_heartbeats(self):
        while self.state == "leader":
            acknowledgments = 1  # Start counting with the leader itself
            entries = []  # Heartbeats have no entries
            append_entries_requests = []

            # Prepare the AppendEntries request
            for node_id, address in self.node_addresses.items():
                if node_id == self.node_id:
                    continue
                append_entries_request = raftkv_pb2.AppendEntriesRequest(
                    term=self.current_term,
                    leaderId=int(self.node_id),
                    prevLogIndex=self.get_last_log_index(),
                    prevLogTerm=self.get_last_log_term(),
                    entries=entries,
                    leaderCommit=self.commit_index,
                    # Not explicitly including lease renewal, but it's implied in heartbeat
                )
                append_entries_requests.append((node_id, address, append_entries_request))

            # Send out heartbeats asynchronously and wait for acknowledgments
            tasks = [self.send_heartbeat(node_id, address, request) for node_id, address, request in append_entries_requests]
            results = await asyncio.gather(*tasks)

            # Count the number of successful acknowledgments
            for success in results:
                if success:
                    acknowledgments += 1

            # Check if lease is renewed by receiving acknowledgments from the majority
            if acknowledgments <= len(self.node_addresses) // 2:
                # Lease not renewed, step down as leader
                print(f"Node {self.node_id} lease renewal failed. Stepping Down.")
                self.state = "follower"
                self.leader_id = None
                return

            await asyncio.sleep(self.heartbeat_interval)

    def AppendEntries(self, request, context):
        # Respond false if term is outdated
        if request.term < self.current_term:
            return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=False)
        
        # Reset election timer on receiving a valid AppendEntries RPC
        self.heartbeat_received = True
        self.leader_id = request.leaderId
        self.current_term = request.term
        self.state = "follower"
        
        # Log consistency check
        if request.prevLogIndex >= len(self.log) or \
        (request.prevLogIndex >= 0 and self.log[request.prevLogIndex]['term'] != request.prevLogTerm):
            return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=False)
        
        # Remove any conflicting entries and append any new ones
        self.log = self.log[:request.prevLogIndex + 1]  # Keep entries up to prevLogIndex
        new_entries = [dict(term=e.term, key=e.key, value=e.value) for e in request.entries]
        for entry in new_entries:
            self.append_entry_to_log(entry)  # Append and save to disk
        
        # If leaderCommit > commitIndex, update commitIndex and apply log entries
        if request.leaderCommit > self.commit_index:
            self.commit_index = min(request.leaderCommit, len(self.log) - 1)
            # Apply log entries to the state machine (not shown here)
            self.commit_entry_to_log()
        
        return raftkv_pb2.AppendEntriesResponse(term=self.current_term, success=True)
    
    def start_election(self):
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.reset_election_timeout()
        self.send_vote_requests()
    
    async def run_election_timer(self):
        while True:
            await asyncio.sleep(self.election_timeout)
            if self.state != "leader":
                self.start_election()

    async def send_vote_requests(self):
        # Assuming an async version of gRPC is used (grpc.aio)
        votes_received = 1  # Start with a vote for self
        for node_id, address in self.node_addresses.items():
            if node_id == self.node_id:
                continue
            async with grpc.aio.insecure_channel(address) as channel:
                stub = raftkv_pb2_grpc.RaftStub(channel)
                try:
                    response = await stub.RequestVote(raftkv_pb2.RequestVoteRequest(
                        term=self.current_term,
                        candidateId=int(self.node_id),
                        lastLogIndex=self.get_last_log_index(),
                        lastLogTerm=self.get_last_log_term(),
                    ))
                    if response.voteGranted:
                        votes_received += 1
                        if votes_received > len(self.node_addresses) // 2:
                            self.become_leader()
                            return
                except grpc.aio.AioRpcError as e:
                    print(f"Error sending vote request to {node_id}: {e}")
    
    async def become_leader(self):
        self.state = "leader"
        self.leader_id = self.node_id
        print(f"Node {self.node_id} became the leader for term {self.current_term}.")
        await self.send_heartbeats()

    async def replicate_log_to_followers(self):
        # Placeholder for log replication logic
        pass

    def reset_election_timeout(self):
        # Reset the election timeout to a new random value
        self.election_timeout = random.uniform(5, 10)

    def get_last_log_index(self):
        # Placeholder for getting the last log index
        return len(self.log) - 1

    def get_last_log_term(self):
        # Placeholder for getting the last log term
        if self.log:
            return self.log[-1]['term']
        return 0

    def handle_client_request(self, request):
        # Logic to append new entries to the log and replicate them
        pass

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
    asyncio.run(serve(node_id, node_addresses))
    