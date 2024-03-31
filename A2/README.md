# RaftKV: A Raft-based Key-Value Store

RaftKV implements a distributed key-value store using the Raft consensus algorithm. This project aims to provide a highly available and consistent storage system, leveraging Raft for log replication and leader election.

## Features

- **Log Replication**: Synchronize log entries across all nodes in the cluster.
- **Leader Election**: Automatically elects a leader among the nodes for log entry commitment.
- **Fault Tolerance**: Handles node failures gracefully, ensuring the system remains available.
- **Client Operations**: Supports basic `SET` and `GET` operations on the key-value store.

## Getting Started

### Prerequisites

- Python 3.x
- gRPC
- Protocol Buffers

### Installation

Clone the repository to your local machine and compile the proto files in both Client and Node folders, you can use the below given command in both folders.

```bash
protoc -I=. --python_out=. --grpc_python_out=. ./raftkv.proto

