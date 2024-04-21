# Distributed K-Means Clustering

This project implements the K-Means clustering algorithm in a distributed manner using Python and gRPC. The implementation leverages the map-reduce programming model to efficiently process large datasets across multiple processes.

## Features

- Distributed computation across mappers and reducers.
- gRPC for inter-process communication.
- Fault tolerance to handle mapper or reducer failures.
- Dynamic configuration through a configuration file.

## Prerequisites

Before you begin, ensure you have installed Python 3.8+ and gRPC:


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


## Implementation Details

- **Master:** Coordinates the overall clustering process, handles failures, and manages communication.
- **Mapper:** Processes input data, assigns points to centroids, and manages data partitioning.
- **Reducer:** Aggregates data from mappers, updates centroid positions, and outputs results.

## Input Data

Place your input data file/s (example: `points.txt`) in the `Data/Input/` directory. The master will distribute this data among mappers based on the configuration.

## Output

- Intermediate data is stored in the `Data/Mappers/` directory under respective mapper directories.
- Final centroids are written to `Data/centroids.txt`.
- Each reducer's output is stored in the `Data/Reducers/` directory.

## Fault Tolerance

The implementation handles two types of failures:
- **Mapper/Reducer Task Failures:** Reassigns tasks to available nodes.
- **Process Failures:** Detects unresponsive processes and restarts tasks.

## Logging

All operations and steps are logged in `dump.txt` for debugging and monitoring. This includes:
- Iteration numbers.
- gRPC calls execution and responses.
- Centroids updates after each iteration.

## Testing

Test cases are provided to ensure the system behaves as expected under various conditions, including simulated failures.

## Contributors

- **Hiren Arvind Kachhadia**

## Acknowledgments

- Inspired by the need for efficient distributed computing solutions in data science.
- Thanks to anyone associated with the project.
