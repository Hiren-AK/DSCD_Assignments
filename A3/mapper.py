import grpc
from concurrent import futures
import pandas as pd
import numpy as np
import os
from mapreduce_pb2 import MapperResponse
import mapreduce_pb2_grpc
import time

class Mapper(mapreduce_pb2_grpc.MapperServiceServicer):
    def ProcessInput(self, request, context):
        try:
            data = pd.read_csv(request.input_data_path)
            centroids = np.array(request.centroids).reshape(-1, data.shape[1])

            # Assign data points to the nearest centroid
            assignments = self.assign_points_to_centroids(data, centroids)
            output_path = self.write_output(assignments, request.task_id)

            return MapperResponse(
                success=True,
                message="Data processed successfully",
                intermediate_data_path=output_path,
                task_id=request.task_id
            )
        except Exception as e:
            return MapperResponse(
                success=False,
                message=str(e),
                intermediate_data_path="",
                task_id=request.task_id
            )

    def assign_points_to_centroids(self, data, centroids):
        distances = np.sqrt(((data - centroids[:, np.newaxis])**2).sum(axis=2))
        closest_centroids = np.argmin(distances, axis=0)
        return pd.DataFrame({'centroid': closest_centroids, 'data': data.tolist()})

    def write_output(self, assignments, task_id):
        output_dir = f"outputs/task_{task_id}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "partitions.csv")
        assignments.to_csv(output_path, index=False)
        return output_path

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapperServiceServicer_to_server(Mapper(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()