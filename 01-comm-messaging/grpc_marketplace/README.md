# gRPC Marketplace

Overview

A gRPC-based marketplace prototype that demonstrates protobuf-defined RPCs for buyers, sellers, a market server, and a notification service.

Stack
- Python 3.x
- gRPC (grpcio, grpcio-tools)

Key files
- market_service.proto, notify_service.proto
- market.py — gRPC server
- buyer.py, seller.py — clients showing RPC flows
- Makefile — helper to compile protos

How to run

```bash
pip install grpcio grpcio-tools
# compile protos in this folder
make all
# start server
python market.py
# in separate terminals run clients
python buyer.py
python seller.py
```

Resume-ready summary
- Built a gRPC marketplace prototype (server + clients), implemented protobuf APIs and end-to-end RPC flows for listings, orders and notifications.
