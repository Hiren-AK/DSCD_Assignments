import grpc
import os
import numpy as np
import asyncio
import shutil  # Import shutil for directory removal
import random
import mapreduce_pb2
import mapreduce_pb2_grpc
from google.protobuf.empty_pb2 import Empty

class MapperServicer(mapreduce_pb2_grpc.MapperServiceServicer):
    def __init__(self):
        self.centroids = None
        self.num_partitions = None             # number of partitions = R (number of reducers)
        self.state = "free"                 # busy, free, OFFLINE: for response to master if CheckHealth was called
        self.local_dir = './Data/Mappers/'  # Directory to store intermediate data
        self.partitions_dir = None          #outputs of partitions
        self.mappers, self.reducers = self.read_config('./config.txt')
        self.key_partition_map = {}
        print("Mapper started")

    def read_config(self, filename):
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
    
    def clean_directory(self, dir_path):
        """ Safely clean the directory specified by dir_path. """
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)  # Remove the directory and all its contents
        os.makedirs(dir_path, exist_ok=True)  # Recreate the directory

    def CheckHealth(self, request, context):
        # Perform health check if needed
        if self.state == 'free':
            is_alive = True # Modify this based on health status
            status_message = self.state
        else:
            is_alive = False
            status_message = self.state
        return mapreduce_pb2.HealthStatus(is_alive=is_alive, status=status_message)
    
    def process_data(self, data):
        output = []
        for point in data:
            nearest_centroid_idx, min_distance = None, float('inf')
            for i, centroid in enumerate(self.centroids):
                distance = np.linalg.norm(centroid - point)
                if distance < min_distance:
                    min_distance, nearest_centroid_idx = distance, i
            output.append((nearest_centroid_idx, point))
            print(f"Point {point} assigned to centroid {nearest_centroid_idx}")
        return output

    def store_output(self, task_id, output):
        mapper_dir = os.path.join(self.local_dir, task_id)
        os.makedirs(mapper_dir, exist_ok=True)
        self.partitions_dir = mapper_dir
        self.partition(output)


    def ProcessInput(self, request, context):
        self.state = 'busy'
        task_id = request.task_id
        mapper_dir = os.path.join(self.local_dir, task_id)
        self.clean_directory(mapper_dir)  # Clear existing data and prepare directory for new output
        self.partitions_dir = mapper_dir
        self.centroids = [np.array(centroid.coordinates) for centroid in request.centroids]
        self.num_partitions = request.num_partitions

        data = []
        try:
            if request.single_input_file:
                print(f"Attempting to load single file: {request.single_input_file}")
                file_data = np.loadtxt(request.single_input_file, delimiter=',')[request.start_index:request.end_index]
                if file_data.size > 0:
                    data.append(file_data)
            else:
                print(f"Input data paths: {request.input_data_paths}")
                for file in request.input_data_paths:
                    if os.path.exists(file) and os.path.getsize(file) > 0:
                        print(f"Loading file: {file}")
                        loaded_data = np.loadtxt(file, delimiter=',')
                        if loaded_data.size > 0:
                            data.append(loaded_data)
                    else:
                        print(f"File not found or empty: {file}")

            if not data:
                print("No valid data found or all files are empty. Sending back success with no action.")
                self.state = 'free'
                return mapreduce_pb2.MapperResponse(success=True, message="No valid data to process.", task_id=task_id)

            combined_data = np.concatenate(data) if len(data) > 1 else data[0]
            output = self.process_data(combined_data)
            self.store_output(task_id, output)
            self.state = 'free'
            # Randomly decide to fail or succeed
            if random.random() < 0.5:
                print("Simulating failure between Master and Mapper")
                return mapreduce_pb2.MapperResponse()  # Empty response to simulate failure
            return mapreduce_pb2.MapperResponse(success=True, message="Data processed successfully.", task_id=task_id)
        except Exception as e:
            print(f"Error processing data in mapper {task_id}: {str(e)}")
            self.state = 'free'
            return mapreduce_pb2.MapperResponse(success=False, message=str(e), task_id=task_id)

    def partition(self, key_values):
        # Partition data
        print("Patritioning the key value pairs...")
        self.key_partition_map = {}
        partitions = {}
        for key, value in key_values:
            partition_idx = key % self.num_partitions  # Determine partition index based on key
            partition_file = os.path.join(self.partitions_dir, f"partition_{partition_idx}.txt")
            if partition_file in partitions.keys():
                partitions[partition_file].append((key, value))
            else:
                partitions[partition_file] = []
                partitions[partition_file].append((key, value))
            self.key_partition_map[key] = partition_file

        # Write key-value pairs to partition files
        for partition_file, pairs in partitions.items():
            with open(partition_file, 'w') as f:  # Open in append mode
                for key, value in pairs:
                    f.write(f"{key},{','.join(map(str, value))}\n")

        print(f"Partitioning done. Total number of partition files: {len(partitions)}")
        return
    
    async def FetchIntermediateData(self, request, context):
        print(f"Data request from {request.task_id} for keys: {request.keys}")
        # Randomly decide to fail or succeed
        if random.random() < 0.5:
            print("Simulating failure between Mapper and Reducer")
            return mapreduce_pb2.FetchResponse()  # Empty response to simulate failure
        response = mapreduce_pb2.FetchResponse()
        for key in request.keys:
            partition_file = self.key_partition_map.get(key, '')
            key_value_pair = response.key_values.add()
            key_value_pair.key = key
            if os.path.exists(partition_file):
                print(f"Sending partion file for centroid id {key} to {request.task_id}")
                with open(partition_file, 'r') as file:
                    for line in file:
                        parts = line.strip().split(',')
                        data_point = mapreduce_pb2.DataPoint(features=list(map(float, parts[1:])))
                        response.key_values.add(key=int(parts[0]), values=[data_point])
        return response

def serve():
    server = grpc.aio.server()
    mapreduce_pb2_grpc.add_MapperServiceServicer_to_server(MapperServicer(), server)
    server.add_insecure_port('[::]:50054')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.start())
    loop.run_forever()

if __name__ == '__main__':
    serve()