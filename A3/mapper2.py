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
    def __init__(self):
        self.is_active = False  # Keep track of the mapper's activity

    def ProcessInput(self, request, context):
        self.is_active = True
        logging.info("Received mapping task with task ID: {}".format(request.task_id))
        try:
            data = pd.read_csv('Data/Input/points.txt', header=None)
            centroids = np.array(request.centroids).reshape(-1, data.shape[1])
            
            # Calculate distances from each data point to each centroid
            distances = np.sqrt(((data.values[:, np.newaxis] - centroids) ** 2).sum(axis=2))
            closest_centroids = np.argmin(distances, axis=1)  # Correct axis to minimize across centroids
            

            output_dir = f"Data/Mappers/M{request.task_id}/"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            for i in range(len(request.centroids)):
                output_path = os.path.join(output_dir, f"partition_{i+1}.txt")
                assigned_data = data.values[closest_centroids == i]
                if assigned_data.size > 0:
                    np.savetxt(output_path, assigned_data, delimiter=",")
            
            logging.info(f"Mapping task {request.task_id} completed, data saved to {output_dir}")
            self.is_active = False
            return mapreduce_pb2.MapperResponse(
                success=True,
                message="Data processed successfully",
                intermediate_data_path=output_dir,
                task_id=request.task_id
            )
        except Exception as e:
            logging.error(f"Error in mapping task {request.task_id}: {e}", exc_info=True)
            return mapreduce_pb2.MapperResponse(
                success=False,
                message=str(e),
                intermediate_data_path="",
                task_id=request.task_id
            )

    def SendData(self, request, context):
        logging.info(f"Attempting to send data for keys {request.keys} from task {request.task_id}")
        response_data = {}
        try:
            for key in request.keys:
                # Ensure keys are integers or are correctly converted to integers
                key_index = int(key)  # Make sure keys are integers or convert them here
                file_path = f"Data/Mappers/M{request.task_id}/partition_{key_index}.txt"
                try:
                    with open(file_path, 'r') as file:
                        response_data[key] = file.read()
                    logging.info(f"Data for key {key} from task {request.task_id} sent successfully")
                except FileNotFoundError as fnf_error:
                    logging.error(f"File not found for task {request.task_id}, key {key}: {fnf_error}")
                    response_data[key] = ""  # Handle file not found by sending empty data or different handling
                except Exception as e:
                    logging.error(f"Error reading file for task {request.task_id}, key {key}: {e}")
                    response_data[key] = ""  # General error handling
            return mapreduce_pb2.FetchResponse(data=response_data)
        except Exception as e:
            logging.error(f"General failure in SendData for task {request.task_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to fetch data for task {request.task_id}: {str(e)}')
            return mapreduce_pb2.FetchResponse(data={})




    def GetStatus(self, request, context):
        # Return the current status of the mapper
        return mapreduce_pb2.StatusResponse(is_running=self.is_active, message="Mapper is active" if self.is_active else "Mapper is idle")


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
