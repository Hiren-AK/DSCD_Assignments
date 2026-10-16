# gRPC Marketplace

A gRPC-based marketplace prototype (server + clients).

Files moved from A1/gRPC:
- Makefile
- market_service.proto
- notify_service.proto
- market_service_pb2.py
- market_service_pb2_grpc.py
- notify_service_pb2.py
- notify_service_pb2_grpc.py
- market.py
- buyer.py
- seller.py

How to run

```bash
cd 01-comm-messaging/grpc_marketplace
make all   # generates _pb2.py and _pb2_grpc.py if needed
python market.py
```
