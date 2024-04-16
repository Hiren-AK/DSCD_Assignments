import grpc
from concurrent import futures
import pandas as pd
import numpy as np
import os
import mapreduce_pb2_grpc
import mapreduce_pb2
import time
import logging


# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler("dump.txt"),
                              logging.StreamHandler()])

class Mapper(mapreduce_pb2_grpc.MapperServiceServicer):
    def ProcessInput(self, request, context):
        logging.info("Received mapping task.")
        try:
            data = pd.read_csv('Data/Input/points.txt', header=None)
            centroids = np.array(request.centroids).reshape(-1, data.shape[1])
            
            # Calculate distances from each data point to each centroid
            distances = np.sqrt(((data.values[:, np.newaxis] - centroids) ** 2).sum(axis=2))
            closest_centroids = np.argmin(distances, axis=1)  # Correct axis to minimize across centroids

            # Output data for each Reducer
            for i in range(len(request.centroids)):
                output_dir = f"Data/Mappers/M{request.task_id}/"
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, f"partition_{i+1}.txt")
                # Filter data points assigned to the i-th centroid
                assigned_data = data.values[closest_centroids == i]
                np.savetxt(output_path, assigned_data, delimiter=",")
            
            logging.info("Mapping task completed successfully.")
            return mapreduce_pb2.MapperResponse(
                success=True,
                message="Data processed successfully",
                intermediate_data_path=output_dir,
                task_id=request.task_id
            )
        except Exception as e:
            logging.error(f"Error processing mapping task: {e}")
            return mapreduce_pb2.MapperResponse(
                success=False,
                message=str(e),
                intermediate_data_path="",
                task_id=request.task_id
            )

    def SendData(self, request, context):
        response_data = {}
        for key in request.keys:
            try:
                # Assuming data for each centroid is stored in a separate file
                with open(f"Data/Mappers/M{self.task_id}/partition_{key}.txt", 'r') as file:
                    response_data[key] = file.read()
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f'Failed to fetch data for key {key}: {str(e)}')
                return mapreduce_pb2.FetchResponse()
        return mapreduce_pb2.FetchResponse(data=response_data)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapperServiceServicer_to_server(Mapper(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
