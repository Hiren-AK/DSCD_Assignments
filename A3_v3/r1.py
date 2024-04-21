import grpc
import os
import numpy as np
import asyncio
from collections import defaultdict
import shutil
import mapreduce_pb2
import mapreduce_pb2_grpc
from google.protobuf.empty_pb2 import Empty

class ReducerServicer(mapreduce_pb2_grpc.ReducerServiceServicer):
    def __init__(self):
        self.state = 'free'  # busy, free, OFFLINE: for response to master if CheckHealth was called
        self.local_dir = './Data/Reducers/'  # Directory to store final output
        self.ensure_directory(self.local_dir)
        self.mappers, self.reducers = self.read_config('./config.txt')
        print("Reducer started, reading configuration...")

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
        print("Configuration loaded: Mappers and Reducers.")
        return mappers, reducers

    def ensure_directory(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

    def CheckHealth(self, request, context):
        print("Health check requested, state:", self.state)
        return mapreduce_pb2.HealthStatus(
            is_alive=self.state == 'free',
            status=self.state
        )

    async def ProcessIntermediateData(self, request, context):
        self.state = 'busy'
        data = defaultdict(list)
        print(f"Processing intermediate data for task ID: {request.task_id}")

        for mapper_id in request.data_paths:
            address = self.mappers[mapper_id]
            channel = grpc.aio.insecure_channel(address)
            stub = mapreduce_pb2_grpc.MapperServiceStub(channel)
            fetch_request = mapreduce_pb2.FetchRequest(keys=request.keys, task_id=request.task_id)
            try:
                fetch_response = await stub.FetchIntermediateData(fetch_request)
                for pair in fetch_response.key_values:
                    for data_point in pair.values:
                        data[pair.key].append(np.array(data_point.features))
            except grpc.aio.AioRpcError as e:
                print(f"Failed to fetch data from mapper {mapper_id}: {str(e)}")

        new_centroids = self.calculate_and_store_centroids(data, request.task_id)
        self.state = 'free'
        return mapreduce_pb2.ReducerResponse(
            success=True,
            message="Data processed successfully.",
            centroids=new_centroids,
            task_id=request.task_id
        )

    def calculate_and_store_centroids(self, data, task_id):
        new_centroids = {}
        output_path = os.path.join(self.local_dir, f"{task_id}.txt")
        with open(output_path, 'w') as file:
            for key, points in data.items():
                if points:
                    centroid = np.mean(np.vstack(points), axis=0)
                    new_centroids[key] = mapreduce_pb2.Centroid(coordinates=centroid.tolist())
                    print(f"New centroid for key {key}: {centroid.tolist()}")
                    file.write(f"{key}: {centroid.tolist()}\n")
                else:
                    print(f"No data points available for key {key}.")
        return new_centroids

def serve():
    server = grpc.aio.server()
    mapreduce_pb2_grpc.add_ReducerServiceServicer_to_server(ReducerServicer(), server)
    server.add_insecure_port('[::]:50051')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.start())
    loop.run_forever()

if __name__ == '__main__':
    serve()
