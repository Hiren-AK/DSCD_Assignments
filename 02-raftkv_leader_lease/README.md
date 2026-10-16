# RaftKV — Leader-lease Raft

Files moved from A2/Nodes and A2/Clients.

raft_nodes contains:
- node.py

raft_clients contains:
- client.py
- raftkv.proto

How to run

```bash
# generate pb files if needed
cd 02-raftkv_leader_lease/raft_clients
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raftkv.proto
# start nodes
python3 ../raft_nodes/node.py <nodeID>
# client
python3 client.py
```
