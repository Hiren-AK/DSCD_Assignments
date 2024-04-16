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
    "reducer1": "50054"
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
    return mapreduce_pb2_grpc.MapperServiceStub(channel)

async def get_reducer_stub(port):
    """Create an asynchronous gRPC stub for a reducer service."""
    channel = grpc.aio.insecure_channel(f'localhost:{port}')
    await channel.channel_ready()  # Ensures the channel is ready for operations
    return mapreduce_pb2_grpc.ReducerServiceStub(channel)

# Asynchronous function to send tasks to a mapper
async def send_task_to_mapper(mapper_name, stub, centroids, input_data_path, task_id):
    try:
        # Flatten the centroids list from list of lists to a single list
        flat_centroids = [coord for centroid in centroids for coord in centroid]
        response = await stub.ProcessInput(
            mapreduce_pb2.MapperTask(
                centroids=flat_centroids,
                input_data_path=input_data_path,
                task_id=task_id
            )
        )
        logging.info(f"Response from {mapper_name}: {response.message}")
        return response
    except grpc.aio.AioRpcError as e:
        logging.error(f"Failed to send task to {mapper_name}: {str(e)}")
        return None

def has_converged(old_centroids, new_centroids, tolerance=0.0001):
    # A simple convergence check based on Euclidean distance between old and new centroids
    total_movement = sum(
        np.linalg.norm(np.array(oc) - np.array(nc))
        for oc, nc in zip(old_centroids, new_centroids)
    )
    return total_movement < tolerance

async def run_iterations(initial_centroids):
    mapper_stubs = {name: await get_mapper_stub(MAPPERS[name]) for name in MAPPERS}
    data_indices = split_data(list(MAPPERS.keys()), DATA_PATH)
    current_centroids = initial_centroids

    for iteration in range(MAX_ITERATIONS):
        logging.info(f"Starting iteration {iteration + 1}")
        # Distributing tasks using the send_task_to_mapper function
        results = await distribute_tasks(mapper_stubs, data_indices, current_centroids)
        if not all(result and result.success for result in results):
            logging.error("One or more mappers failed, aborting this iteration")
            continue  # or break, based on your error handling strategy

        # Logic to process results, update centroids, and check for convergence
        new_centroids = process_mapper_outputs(results)  # Define this function as needed
        if has_converged(current_centroids, new_centroids):
            logging.info("Convergence reached.")
            break
        current_centroids = new_centroids

    logging.info("Final centroids: {}".format(current_centroids))

async def main():
    num_mappers, num_reducers, num_centroids, max_iterations = get_input_parameters()
    initial_centroids = random_initial_centroids(data_path, num_centroids)
    logging.info(f"Randomly initialized centroids: {initial_centroids}")

    mapper_stubs = {f"mapper{i}": await get_mapper_stub(f'5005{i+1}') for i in range(num_mappers)}
    data_indices = split_data(num_mappers, data_path)

    for iteration in range(max_iterations):
        logging.info(f"Starting iteration {iteration + 1}")
        results = await distribute_tasks(mapper_stubs, data_indices, initial_centroids)
        # Assuming results are processed and new centroids are calculated here
        # Check for convergence and possibly break the loop if converged

    logging.info("Final centroids: {}".format(initial_centroids))

if __name__ == '__main__':
    asyncio.run(main())