import grpc
import os
import numpy as np
import asyncio
import mapreduce_pb2
import mapreduce_pb2_grpc
from google.protobuf.empty_pb2 import Empty

class MapperServicer(mapreduce_pb2_grpc.MapperServiceServicer):
    def __init__(self, centroids):
        self.centroids = None
        self.num_partitions = None             # number of partitions = R (number of reducers)
        self.state = "free"                 # busy, free, OFFLINE: for response to master if CheckHealth was called
        self.local_dir = './Data/Mappers/'  # Directory to store intermediate data
        self.partitions_dir = None          #outputs of partitions
        self.mappers, self.reducers = self.read_config('./Data/config.txt')
        print("Mapper started")

    def read_config(filename):
        mappers = {}
        reducers = {}
        with open(filename, 'r') as file:
            for line in file:
                if line.startswith('#') or not line.strip():
                    continue
                parts = line.strip().split(',')
                service_id = parts[0]
                host = parts[1]
                port = parts[2]
                if 'M' in service_id:
                    mappers[service_id] = host + ':' + port
                elif 'R' in service_id:
                    reducers[service_id] = host + ':' + port
        return mappers, reducers

    async def CheckHealth(self, request, context):
        # Perform health check if needed
        if self.state == 'free':
            is_alive = True # Modify this based on health status
            status_message = self.state
        else:
            is_alive = False
            status_message = self.state
        return mapreduce_pb2.HealthStatus(is_alive=is_alive, status=status_message)

    async def ProcessInput(self, request, context):
        self.state = 'busy'
        task_id = request.task_id
        self.centroids = request.centroids
        input_data_paths = request.input_data_paths
        single_input_file = request.single_input_file
        start_index = request.start_index
        end_index = request.end_index
        self.num_partitions = request.num_partitions

        print(f"Mapper {task_id} recieved ProcessInput request with centroids {self.centroids}")

        # Load input data
        if single_input_file:
            data = np.loadtxt(single_input_file, delimiter=',')
            data = data[start_index:end_index]
        else:
            data = np.concatenate([np.loadtxt(file, delimiter=',') for file in input_data_paths])

        # Process data
        output = []
        for point in data:
            # Find the nearest centroid
            nearest_centroid_idx = None
            min_distance = float('inf')

            # Find the index of the nearest centroid
            for i, centroid in enumerate(self.centroids):
                distance = np.linalg.norm(centroid - point)
                if distance < min_distance:
                    min_distance = distance
                    nearest_centroid_idx = i

            output.append((nearest_centroid_idx, point))

        print("Finished making key-value pairs...")

        # Create directories if not exist
        mapper_dir = os.path.join(self.local_dir, task_id)
        os.makedirs(mapper_dir, exist_ok=True)
        self.partitions_dir = mapper_dir

        try:
            await self.partition(output)
        except:
            print("Ran into an issue while partitioning")
        
        self.state = 'free'

        return mapreduce_pb2.MapperResponse(success=True, message=f"Mapper {task_id} processed data.", task_id=task_id)

    async def partition(self, key_values):
        # Partition data
        print("Patritioning the key value pairs...")
        partitions = {f"partition_{i}.txt": [] for i in range(self.num_partitions)}
        for key, value in key_values:
            partition_idx = hash(key) % self.num_partitions  # Determine partition index based on key
            partition_file = os.path.join(self.partitions_dir, f"partition_{partition_idx}.txt")
            partitions[partition_file].append((key, value))

        # Write key-value pairs to partition files
        for partition_file, pairs in partitions.items():
            with open(partition_file, 'w') as f:  # Open in append mode
                for key, value in pairs:
                    f.write(f"{key},{','.join(map(str, value))}\n")

        print(f"Partitioning done. Total number of partition files: {len(partitions)}")


def serve():
    server = grpc.aio.server()
    mapreduce_pb2_grpc.add_MapperServiceServicer_to_server(MapperServicer(), server)
    server.add_insecure_port('[::]:50051')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.start())
    loop.run_forever()

if __name__ == '__main__':
    serve()