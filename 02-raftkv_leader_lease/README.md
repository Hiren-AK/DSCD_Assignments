# RaftKV — Leader-lease Raft

Overview

Distributed key-value store implementing Raft consensus with a leader-lease optimization, persistent node storage, and GET/SET RPCs via gRPC.

Stack
- Python 3.x
- gRPC (grpcio, grpcio-tools)

Key files
- raftkv.proto — API definition
- node.py — Raft node implementation (leader election, log replication, leader lease handling)
- client.py — CLI client to issue GET/SET

How to run

```bash
pip install grpcio grpcio-tools
# in both raft clients and nodes folders
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raftkv.proto
# start nodes (one per process / VM)
python3 node.py <nodeID>
# start client
python3 client.py
```

Notes
- The original A2 README contains additional deployment notes for Google Cloud VMs and firewall rules.

Resume-ready summary
- Implemented a fault-tolerant RaftKV store with leader-lease optimization, persistent storage and gRPC APIs, tested across multi-node deployments.
