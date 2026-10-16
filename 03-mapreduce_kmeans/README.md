# MapReduce K-means

Overview

MapReduce-style framework to parallelize K-means clustering across mapper and reducer worker processes coordinated by a Master.

Stack
- Python 3.x
- gRPC/protobuf (mapreduce.proto present)

Key files
- Master.py — coordinator
- mapper1.py, mapper2.py, mapper3.py — mapper workers
- reducer1.py, reducer2.py, reducer3.py — reducer workers
- mapreduce.proto + generated bindings
- Data/ — datasets used for clustering

How to run

```bash
# start master
python3 Master.py
# start mappers and reducers in separate terminals/processes
python3 mapper1.py & python3 mapper2.py & python3 mapper3.py
python3 reducer1.py & python3 reducer2.py & python3 reducer3.py
```

Resume-ready summary
- Built a MapReduce framework to run K-means clustering, coordinating master, mappers and reducers using protobuf/gRPC bindings.
