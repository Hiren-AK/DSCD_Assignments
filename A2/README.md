# RaftKV: Distributed Key-Value Store using Raft with Leader Lease

RaftKV is a fault-tolerant, distributed key-value store that implements the Raft consensus algorithm, enhanced with leader lease modifications for improved performance and reliability. This project is designed to run across multiple nodes, each hosted on a separate Virtual Machine on Google Cloud, allowing it to efficiently handle storage and retrieval of key-value pairs in a consistent manner, even in the face of node failures.

## Overview

The implementation focuses on:

- Persistent node storage, ensuring data integrity across restarts.
- Leader lease mechanism for handling read and write operations with guaranteed consistency.
- Support for `SET` and `GET` operations on the distributed key-value database.
- Use of gRPC for node-to-node and client-to-node communication.

## Getting Started

### Prerequisites
```bash
pip install grpcio grpcio-tools
```

- Google Cloud Platform account and VM instances setup.
- Python 3.x and pip installed on all nodes.
- gRPC and Protocol Buffers installed. (Note: These are the only allowed communication libraries for this project).

### Installation

Clone the repository to your local machine and compile the proto files in both Client and Node folders, you can use the below given command in both folders.

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raftkv.proto
```
Configuration
Configure each node and client with the IP addresses and ports of all participating nodes.
Ensure your Google Cloud VM firewall and network settings allow TCP communication over the ports used by your RaftKV instances.
Running a Node

To start a node on each VM, run:
```bash
python3 node.py <nodeID>
```
(Replace <nodeID> with a unique identifier for each node.)

Running the Client
To interact with the RaftKV cluster from a client:
```bash
python3 client.py
```
