import grpc
from concurrent import futures
import time
import logging
import asyncio

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

mapper_stubs = create_mapper_stubs()


mapper_stubs = create_mapper_stubs()


class Reducer(mapreduce_pb2_grpc.ReducerServiceServicer):

    async def fetch_data_from_all_mappers(mapper_stubs, keys):
        tasks = []
        for stub in mapper_stubs:
            request = mapreduce_pb2.FetchRequest(keys=keys)
            task = stub.SendData(request)
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        data = {}
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Failed to fetch data from mapper: {str(result)}")
            else:
                data.update(result.data)
        return data


    async def process_intermediate_data(self, request, context):
        logging.info("Received request to process intermediate data for task ID: {}".format(request.task_id))
        try:
            mapper_stubs = create_mapper_stubs()  # Create or retrieve a list of stubs
            data = await fetch_data_from_all_mappers(mapper_stubs.values(), request.data_paths)
            centroids = {}
            for centroid_id, points_str in data.items():
                points = np.fromstring(points_str, sep=',').reshape(-1, 2)
                centroids[centroid_id] = points

            new_centroids = {cid: np.mean(pts, axis=0).tolist() for cid, pts in centroids.items()}
            output_data_path = f"Data/Reducers/R{request.task_id}.txt"
            np.savetxt(output_data_path, np.array(list(new_centroids.values())), delimiter=",")
            logging.info(f"Reducer saved new centroids at {output_data_path}")
            return mapreduce_pb2.ReducerResponse(success=True, message="Reduction completed successfully", output_data_path=output_data_path, task_id=request.task_id)
        except Exception as e:
            logging.error(f"Error in processing intermediate data: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return mapreduce_pb2.ReducerResponse(success=False, message=str(e), task_id=request.task_id)

    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_ReducerServiceServicer_to_server(Reducer(), server)
    server.add_insecure_port('[::]:50054')
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
