import grpc
from concurrent import futures
import time
import logging
import asyncio
import numpy as np
import os


import mapreduce_pb2
import mapreduce_pb2_grpc

# Setup logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("reducer.log"), logging.StreamHandler()])

def get_mapper_stub(address):
    """ Helper function to create a stub for connecting to the mapper service. """
    channel = grpc.aio.insecure_channel(address)
    return mapreduce_pb2_grpc.MapperServiceStub(channel)

def create_mapper_stubs():
    mapper_ports = {
        "mapper1": "50051",
        "mapper2": "50052",
        "mapper3": "50053",
    }
    mapper_stubs = {}
    for mapper, port in mapper_ports.items():
        channel = grpc.aio.insecure_channel(f'localhost:{port}')
        stub = mapreduce_pb2_grpc.MapperServiceStub(channel)
        mapper_stubs[mapper] = stub
    return mapper_stubs

def process_data(data):
    # Check if the data array is empty
    if data.size == 0:
        logging.warning("Attempted to process empty data slice.")
        return None  # or handle as appropriate for your application

    # Calculate the mean of the data points
    try:
        centroid = np.mean(data, axis=0)
        logging.info(f"Calculated new centroid: {centroid}")
        return centroid
    except Exception as e:
        logging.error("Error calculating centroid:", exc_info=True)
        return None


class Reducer(mapreduce_pb2_grpc.ReducerServiceServicer):

    async def FetchData(self, keys, mapper_stubs, task_id):
        """ Fetches data from mappers based on keys and task_id provided in the request. """
        logging.info(f"Fetching data with keys: {keys} for task ID: {task_id}")
        tasks = [stub.SendData(mapreduce_pb2.FetchRequest(keys=keys, task_id=task_id)) for stub in mapper_stubs.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        data = {}
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Failed to fetch data from mapper: {result}")
            else:
                data.update(result.data)
        return data


    async def ProcessIntermediateData(self, request, context):
        logging.info(f"Received request to process intermediate data for task ID: {request.task_id}")
        try:
            # Dynamically creating mapper stubs
            mapper_stubs = create_mapper_stubs()
            num_reducers = request.num_reducers
            partition_keys = [str(i + 1) for i in range(num_reducers)]  # Generating partition keys

            # Fetching data from mappers using the task_id from the request
            fetched_data = await self.FetchData(partition_keys, mapper_stubs, request.task_id)
            if not fetched_data:
                raise ValueError("No data received from mappers.")

            # Process fetched data to calculate new centroids
            centroids = {}
            for key, points_str in fetched_data.items():
                # Converting string data back to numpy array
                points = np.fromstring(points_str, sep=',').reshape(-1, 2)
                if points.size > 0:
                    centroids[int(key)] = points
                else:
                    logging.warning(f"No data points associated with key {key}")

            if not centroids:
                raise ValueError("No valid data points received for processing.")

            # Calculating new centroids
            new_centroids = {cid: np.mean(pts, axis=0).tolist() for cid, pts in centroids.items() if len(pts) > 0}
            output_directory = f"Data/Reducers/"
            os.makedirs(output_directory, exist_ok=True)  # Ensure directory exists
            output_data_path = os.path.join(output_directory, f"R{request.task_id}.txt")

            # Saving the new centroids to a file
            np.savetxt(output_data_path, np.array(list(new_centroids.values())), delimiter=",")
            logging.info(f"Reducer saved new centroids at {output_data_path}")

            return mapreduce_pb2.ReducerResponse(success=True, message="Reduction completed successfully", output_data_path=output_data_path, task_id=request.task_id)

        except ValueError as ve:
            logging.error(f"ValueError in processing: {ve}")
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(str(ve))
            return mapreduce_pb2.ReducerResponse(success=False, message=str(ve), task_id=request.task_id)

        except Exception as e:
            logging.error(f"Unexpected error in processing intermediate data: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return mapreduce_pb2.ReducerResponse(success=False, message=str(e), task_id=request.task_id)






async def serve():
    server = grpc.aio.server()
    mapreduce_pb2_grpc.add_ReducerServiceServicer_to_server(Reducer(), server)
    server.add_insecure_port('[::]:50054')
    await server.start()
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        await server.stop(None)

if __name__ == '__main__':
    asyncio.run(serve())