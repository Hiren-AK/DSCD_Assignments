# ZeroMQ Group Messaging

Overview

Group messaging application implemented using ZeroMQ patterns to enable group broadcasts and routed messaging between clients.

Stack
- Python 3.x
- pyzmq (ZeroMQ)

Key files
- Message_server.py — central message router
- Group.py — group management logic
- user.py — client interface

How to run

1) Install pyzmq:

```bash
pip install pyzmq
```

2) Start the server and clients according to the scripts in this folder.

Resume-ready summary
- Implemented a ZeroMQ-based group chat demonstrating pub/sub and routing patterns for low-latency messaging.
