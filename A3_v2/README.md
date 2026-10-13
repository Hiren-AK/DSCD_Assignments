
## Compilation

Compile the `.proto` files to generate the necessary gRPC code:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mapreduce.proto
```


## Configuration

Adjust the `config.txt` file to define the number of mappers and reducers, including their respective ports. Example format:
```bash
M1,localhost,50051
M2,localhost,50052
R1,localhost,50061
R2,localhost,50062
```


## Running the Project

To start the master program with a specified number of clusters (K) and iterations (i), use the following command:

```bash
python Master.py K i
```
