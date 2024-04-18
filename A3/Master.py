import asyncio
import grpc.aio
import mapreduce_pb2
import mapreduce_pb2_grpc
import logging
import numpy as np
import pandas as pd

# ports for each mapper
MAPPERS = {
    "mapper1": "50051",
    "mapper2": "50052",
    "mapper3": "50053",
}

mapper_ports = ["mapper1", "mapper2", "mapper3"]

REDUCERS = {
    "reducer1": "50054",
    "reducer2": "50055"
}

data_path = 'Data/Input/points.txt'

def get_input_parameters():
    """
    Prompts the user to input necessary configuration parameters.
    """
    num_mappers = int(input("Enter the number of mappers: "))
    num_reducers = int(input("Enter the number of reducers: "))
    num_centroids = int(input("Enter the number of centroids: "))
    max_iterations = int(input("Enter the maximum number of K-Means iterations: "))
    return num_mappers, num_reducers, num_centroids, max_iterations


# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler("dump.txt"),
                              logging.StreamHandler()])

def random_initial_centroids(data_path, k):
    """
    Randomly selects 'k' centroids from a given dataset.
    """
    try:
        data = pd.read_csv(data_path, header=None)
        centroids = data.sample(n=k).values.tolist()
        return centroids
    except Exception as e:
        logging.error(f"Error reading the data or selecting centroids: {str(e)}")
        sys.exit("Failed to initialize centroids.")

async def distribute_tasks(mapper_stubs, data_indices, centroids):
    tasks = []
    # Loop through each stub and corresponding indices
    for mapper_name, stub in mapper_stubs.items():
        start, end = data_indices[mapper_name]
        # Use the send_task_to_mapper function to dispatch tasks
        task = send_task_to_mapper(mapper_name, stub, centroids, data_path, start)  # task_id can be 'start' or a unique identifier
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

def split_data(num_mappers, data_path):
    """
    Splits the data indices and assigns them to mappers.
    """
    data = pd.read_csv(data_path, header=None)
    total_points = len(data)
    chunk_size = total_points // num_mappers
    data_indices = {f"mapper{i}": (i * chunk_size, (i + 1) * chunk_size if i < num_mappers - 1 else total_points)
                    for i in range(num_mappers)}
    return data_indices


# Helper function to get a gRPC stub for a given mapper
async def get_mapper_stub(port):
    channel = grpc.aio.insecure_channel(f'localhost:{port}')
    await channel.channel_ready()
    logging.info(f"Connected to mapper at port {port}")
    return mapreduce_pb2_grpc.MapperServiceStub(channel)


async def get_reducer_stubs(num_reducers):
    reducer_stubs = {}
    for i in range(1, num_reducers + 1):
        port = REDUCERS[f"reducer{i}"]
        channel = grpc.aio.insecure_channel(f'localhost:{port}')
        await channel.channel_ready()
        reducer_stubs[f"reducer{i}"] = mapreduce_pb2_grpc.ReducerServiceStub(channel)
    return reducer_stubs


# Asynchronous function to send tasks to a mapper
async def send_task_to_mapper(mapper_name, stub, centroids, input_data_path, task_id, num_reducers):
    try:
        flat_centroids = [coord for centroid in centroids for coord in centroid]
        response = await stub.ProcessInput(
            mapreduce_pb2.MapperTask(
                centroids=flat_centroids,
                input_data_path=input_data_path,
                task_id=task_id,
                num_reducers=num_reducers  # Sending the number of reducers to mappers
            )
        )
        logging.info(f"Response from {mapper_name}: {response.message}")
        return response
    except grpc.aio.AioRpcError as e:
        logging.error(f"Failed to send task to {mapper_name}: {str(e)}")
        return None


async def send_task_to_reducers(reducer_stubs, data_paths, iteration):
    tasks = []
    # Prepare tasks for all reducers
    for reducer_name, stub in reducer_stubs.items():
        task = stub.ProcessIntermediateData(
            mapreduce_pb2.ReducerTask(
                data_paths=data_paths, 
                task_id=iteration,
                num_reducers=num_reducers
            )
        )
        tasks.append(task)

    try:
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        # Filter and process successful responses
        for response in responses:
            if isinstance(response, Exception):
                logging.error(f"Reducer task failed with exception: {str(response)}")
                continue
            if response.success:
                logging.info(f"Reducer {reducer_name} processed data: {response.message}")
                return response.output_data_path  # Assuming we only need the first successful response
            else:
                logging.error(f"Reducer {reducer_name} failed to process data: {response.message}")
        logging.error("No successful reducer responses were received.")
        return None
    except Exception as e:
        logging.error(f"Failed to send task to reducers: {str(e)}")
        return None

async def wait_for_mappers(mapper_stubs):
    """Wait for all mappers to complete their tasks by checking if they are not running."""
    # Continuously check if all mappers are done
    while True:
        tasks = [stub.GetStatus(mapreduce_pb2.StatusRequest()) for stub in mapper_stubs.values()]
        responses = await asyncio.gather(*tasks)
        # Check if all mappers are not running
        if all(not response.is_running for response in responses):
            break  # Exit the loop if all mappers are done
        await asyncio.sleep(1)  # Wait a bit before checking again

    return responses





def has_converged(old_centroids, new_centroids, tolerance=0.0001):
    # A simple convergence check based on Euclidean distance between old and new centroids
    total_movement = sum(
        np.linalg.norm(np.array(oc) - np.array(nc))
        for oc, nc in zip(old_centroids, new_centroids)
    )
    return total_movement < tolerance

async def run_master(num_mappers, num_reducers, num_centroids, max_iterations):
    initial_centroids = random_initial_centroids(data_path, num_centroids)
    if not initial_centroids:
        logging.error("Centroids initialization failed. Exiting.")
        return
    
    logging.info("Randomly initialized centroids: %s", initial_centroids)

    mapper_stubs = {f"mapper{i+1}": await get_mapper_stub(MAPPERS[f"mapper{i+1}"]) for i in range(num_mappers)}
    reducer_stubs = await get_reducer_stubs(num_reducers)
    current_centroids = initial_centroids

    for iteration in range(max_iterations):
        logging.info("Starting iteration %d", iteration + 1)
        mapper_responses = await asyncio.gather(*(
            send_task_to_mapper(
                f"mapper{i+1}", 
                mapper_stubs[f"mapper{i+1}"], 
                current_centroids, 
                data_path,  # passing the input data path
                iteration,
                num_reducers
            ) for i in range(num_mappers)
        ))

        if all(response and response.success for response in mapper_responses):
            data_paths = [response.intermediate_data_path for response in mapper_responses if response]
            await wait_for_mappers(mapper_stubs)  # Wait for all mappers to complete their tasks
            new_centroids = await send_task_to_reducers(reducer_stubs, data_paths, iteration)

            if new_centroids is None:
                logging.error("Failed to obtain new centroids from reducers.")
                continue


            if has_converged(current_centroids, new_centroids):
                logging.info("Convergence reached.")
                break
            current_centroids = new_centroids
        else:
            logging.error("One or more mappers failed, aborting this iteration.")

    logging.info("Final centroids: %s", current_centroids)

async def main():
    num_mappers, num_reducers, num_centroids, max_iterations = get_input_parameters()
    await run_master(num_mappers, num_reducers, num_centroids, max_iterations)

if __name__ == '__main__':
    asyncio.run(main())