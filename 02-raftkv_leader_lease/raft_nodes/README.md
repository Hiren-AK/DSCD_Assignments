# Raft Nodes

This folder contains node implementation files for the RaftKV assignment.

Key file
- node.py — Raft node process (leader election, log replication, persistence)

How to run

```bash
python3 node.py <nodeID>
```

Resume-ready summary
- Node process implementing Raft state machine, leader election and persistence.
