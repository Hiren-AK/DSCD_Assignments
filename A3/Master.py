import asyncio
import grpc
import grpc.aio
import mapreduce_pb2
import mapreduce_pb2_grpc
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler("dump.txt"),
                              logging.StreamHandler()])

async def run_master(centroids, num_iterations):
    # Set up asynchronous channels to mappers and reducers
    mapper_channel = grpc.aio.insecure_channel('localhost:50051')
    reducer_channel = grpc.aio.insecure_channel('localhost:50052')
    
    mapper_stub = mapreduce_pb2_grpc.MapperServiceStub(mapper_channel)
    reducer_stub = mapreduce_pb2_grpc.ReducerServiceStub(reducer_channel)

    current_centroids = centroids
    
    for iteration in range(num_iterations):
        logging.info(f"Starting iteration {iteration+1}")
        
        # Asynchronous call to Mapper
        try:
            mapper_response = await mapper_stub.ProcessInput(
                mapreduce_pb2.MapperTask(centroids=current_centroids, input_data_path="path/to/data", task_id=iteration)
            )
            logging.info(f"gRPC call to Mapper successful: {mapper_response.message}")
        except grpc.aio.AioRpcError as e:
            logging.error(f"gRPC call to Mapper failed: {str(e)}")
            continue

        if mapper_response.success:
            try:
                # Asynchronous call to Reducer
                reducer_response = await reducer_stub.ProcessIntermediateData(
                    mapreduce_pb2.ReducerTask(data_paths=[mapper_response.intermediate_data_path], task_id=iteration)
                )
                logging.info(f"gRPC call to Reducer successful: {reducer_response.message}")
                
                # Update centroids
                current_centroids = read_new_centroids(reducer_response.output_data_path)
                logging.info(f"New centroids: {current_centroids}")
            except grpc.aio.AioRpcError as e:
                logging.error(f"gRPC call to Reducer failed: {str(e)}")
        else:
            logging.error("Mapper task failed, aborting this iteration.")

        logging.info(f"End of iteration {iteration+1}")

async def main():
    initial_centroids = [random_initial_centroids()]  # Define this function based on your data
    logging.info(f"Randomly initialized centroids: {initial_centroids}")
    await run_master(initial_centroids, 10)

if __name__ == '__main__':
    asyncio.run(main())
