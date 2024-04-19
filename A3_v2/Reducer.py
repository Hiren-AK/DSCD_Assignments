import os
import grpc
import asyncio
import numpy as np
from collections import defaultdict
import mapreduce_pb2
import mapreduce_pb2_grpc
from google.protobuf.empty_pb2 import Empty

class ReducerServicer(mapreduce_pb2_grpc.ReducerServiceServicer):
    def __init__(self):
        self.state = "free"  # busy, free, OFFLINE: for response to master if CheckHealth was called
        self.local_dir = './Data/Reducers/'  # Directory to store final output
        self.mappers, self.reducers = self.read_config('./config.txt')
        print("Reducer started")

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

    async def CheckHealth(self, request, context):
        # Perform health check
        if self.state == 'free':
            is_alive = True # Modify this based on health status
            status_message = self.state
        else:
            is_alive = False
            status_message = self.state
        return mapreduce_pb2.HealthStatus(is_alive=is_alive, status=status_message)

    async def ProcessIntermediateData(self, request, context):
        self.state = 'busy'
        data = defaultdict(list)

        # Assume `active_mappers` and fetching logic as previously defined
        for mapper_id in request.data_paths:  # `data_paths` now stores active mappers
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
                continue

        # Calculate new centroids
        new_centroids = {}
        for key, points in data.items():
            if points:
                new_centroids[key] = np.mean(np.vstack(points), axis=0).tolist()
            else:
                new_centroids[key] = []  # Or some default value if no points were found

        self.state = 'free'
        return mapreduce_pb2.ReducerResponse(
            success=True,
            message="Data processed successfully.",
            centroids=new_centroids,
            task_id=request.task_id
        )

def serve():
    server = grpc.aio.server()
    mapreduce_pb2_grpc.add_ReducerServiceServicer_to_server(ReducerServicer(), server)
    server.add_insecure_port('[::]:50053')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.start())
    loop.run_forever()

if __name__ == '__main__':
    serve()
