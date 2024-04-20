import sys
import numpy as np
import os
import grpc
import asyncio
import time
import mapreduce_pb2
import mapreduce_pb2_grpc
from google.protobuf.empty_pb2 import Empty
from collections import defaultdict
# Additional imports for gRPC might be necessary

class Master:
    def __init__(self, num_centroids, max_iterations):
        self.num_centroids = num_centroids
        self.max_iterations = max_iterations
        self.input_data_dir = './Data/Input/'
        self.centroids = None
        self.new_centroids = None
        self.new_centroids_count = None
        self.converged = False
        self.single_file_input = True
        self.file_paths = []
        self.single_file_path = None  # To store the path of the single input file
        self.dumpfile = './Data/centroids.txt'
        self.outputfile = './Data/output.txt'
        self.reducer_responses = []
        self.mappers, self.reducers = self.read_config('./config.txt')  # Adjusted to call with default config path
        self.alive_mappers, self.alive_reducers = self.monitor_health()

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
    
    def dump_centroids(self, convergence):
        with open(self.dumpfile, 'a') as f:  # Open in append mode
            f.write(f"Iteration centroids: {self.centroids}\n")
        if convergence:
            with open(self.outputfile, 'w') as f:  # Open in append mode
                f.write(f"Centroids: {self.centroids}\n")

    def initialize_centroids(self, data):
        # Randomly choose centroids from the data points
        indices = np.random.choice(len(data), self.num_centroids, replace=False)
        self.centroids = data[indices]
        print(f"Initial centroids: {self.centroids}")
        
    def load_data(self):
        try:
            # Get all file paths in the directory
            all_files = [os.path.join(self.input_data_dir, f) for f in os.listdir(self.input_data_dir) if os.path.isfile(os.path.join(self.input_data_dir, f))]
        except Exception as e:
            print(f"Ran into issue while loading data: {str(e)}")
            return None, False

        files = [filename for filename in all_files if filename.endswith(".txt")]

        if len(files) == 1:
            # Load data from a single file
            print("single file input")
            data = np.loadtxt(files[0], delimiter=',')
            self.single_file_input = True
            self.single_file_path = files[0]  # Store the path of the single input file
            return data, False
        elif len(files) > 1:
            print("multiple files input")
            self.single_file_input = False
            data_list = []
            for file in files:
                self.file_paths.append(file)
                data = np.loadtxt(file, delimiter=',')
                data_list.append(data)
            return np.vstack(data_list), True
        else:
            print("Input file/s missing")
            return None

    def split_data(self, data):
        active_mappers, _ = self.monitor_health()
        num_active_mappers = len(active_mappers)
        data_splits = []

        if self.single_file_input:
            # Split single file data by index for each mapper
            num_data_points = len(data)
            indices_per_mapper = num_data_points // num_active_mappers
            extra_data = num_data_points % num_active_mappers
            start_index = 0

            for mapper_id in active_mappers:
                end_index = start_index + indices_per_mapper + (1 if extra_data > 0 else 0)
                if extra_data > 0:
                    extra_data -= 1
                data_splits.append((mapper_id, [self.single_file_path], start_index, end_index))
                start_index = end_index
        else:
            # Distribute file paths among mappers
            files_per_mapper = len(data) // num_active_mappers
            extra_files = len(data) % num_active_mappers
            data_index = 0

            for mapper_id in active_mappers:
                end_index = data_index + files_per_mapper + (1 if extra_files > 0 else 0)
                files_for_this_mapper = data[data_index:end_index]
                data_splits.append((mapper_id, files_for_this_mapper, 0, -1))  # No indices needed for multiple files
                data_index = end_index
                if extra_files > 0:
                    extra_files -= 1

        return data_splits


        
    async def dispatch_and_process_tasks(self, data_splits):
        tasks = []

        for split_info in data_splits:
            mapper_id = split_info[0]
            files = split_info[1] if len(split_info[1]) else ['']
            start_index = split_info[2] if len(split_info) > 2 else -1
            end_index = split_info[3] if len(split_info) > 3 else -1
            task = mapreduce_pb2.MapperTask(
                centroids=[mapreduce_pb2.Centroid(coordinates=list(centroid)) for centroid in self.centroids],
                input_data_paths=files,
                single_input_file='',
                start_index=start_index,
                end_index=end_index,
                task_id=str(mapper_id),
                num_partitions=len(self.alive_reducers.keys())
            )
            tasks.append(self.process_single_task(task, mapper_id))

        # Await all tasks at once to run them in parallel
        results = await asyncio.gather(*tasks)
        print(results)

        # After all tasks are completed, check the results
        if all(result and result.success for result in results):  # Check if all results are successful and not None
            print("All tasks completed successfully.")
            return True
        else:
            print("Some tasks failed or were None.")
            return False

    async def process_single_task(self, task, mapper_id):
        address = self.mappers[mapper_id]
        async with grpc.aio.insecure_channel(address) as channel:
            stub = mapreduce_pb2_grpc.MapperServiceStub(channel)
            try:
                response = await stub.ProcessInput(task)
                print(f"Task sent to mapper {mapper_id}. Response: {response.message}")
                return response
            except grpc.aio.AioRpcError as e:
                print(f"Failed to send task to mapper {mapper_id}: {str(e)}")
                return None  # Return None to indicate failure


    def monitor_health(self):
        # Check the health of mappers
        active_mappers = {}
        for mapper_id, address in self.mappers.items():
            channel = grpc.insecure_channel(address)
            stub = mapreduce_pb2_grpc.MapperServiceStub(channel)
            try:
                response = stub.CheckHealth(Empty())
                if response.is_alive and response.status == "free":
                    active_mappers[mapper_id] = address
                elif response.status == "busy":
                    print(f"mapper {mapper_id} is busy")
            except grpc.RpcError:
                print(f"Mapper {mapper_id} at {address} is not responding.")

        # Check the health of reducers
        active_reducers = {}
        for reducer_id, address in self.reducers.items():

            channel = grpc.insecure_channel(address)
            stub = mapreduce_pb2_grpc.ReducerServiceStub(channel)
            try:
                response = stub.CheckHealth(Empty())
                if response.is_alive and response.status == "free":
                    active_reducers[reducer_id] = address
                elif response.status == "busy":
                    print(f"reducer {reducer_id} is busy")
            except grpc.RpcError:
                print(f"Reducer {reducer_id} at {address} is not responding.")

        return active_mappers, active_reducers
    
    async def invoke_reducers(self):
        self.reducer_responses = []
        keys_per_reducer = defaultdict(list)
        centroid_indices = list(range(self.num_centroids))

        for i, key in enumerate(centroid_indices):
            reducer_id = list(self.alive_reducers.keys())[i % len(self.alive_reducers.keys())]
            keys_per_reducer[reducer_id].append(key)

        print(f"keys for reducers for invoking: \n {keys_per_reducer}")

        tasks = []
        for reducer_id, address in self.alive_reducers.items():
            task = self.process_reducer_task(
                list(self.alive_mappers.keys()),
                keys_per_reducer[reducer_id],
                reducer_id,
                address
            )
            tasks.append(task)
            print("here2")
            print(task)

        if tasks:  # Ensure there are tasks to await
            responses = await asyncio.gather(*tasks)
            time.sleep(2)
            for response in responses:
                if response:
                    self.reducer_responses.append(response)
                    print(f"Received response: {response.message}")
                else:
                    print(f"Response for a task was None, indicating an issue or a failed task.")
            print("All reducer tasks completed and responses stored.")
        else:
            print("No reducer tasks were initiated.")

    async def process_reducer_task(self, active_mappers, keys, reducer_id, address):
        async with grpc.aio.insecure_channel(address) as channel:
            stub = mapreduce_pb2_grpc.ReducerServiceStub(channel)
            task = mapreduce_pb2.ReducerTask(
                data_paths=active_mappers,
                keys=keys,
                task_id=reducer_id
            )
            try:
                response = await stub.ProcessIntermediateData(task)
                print(f"Task sent to reducer {reducer_id}. Response: {response.message}")
                return response
            except grpc.aio.AioRpcError as e:
                print(f"Failed to send task to reducer {reducer_id}: {str(e)}")
                return None
    
    async def handle_reducer_responses(self):
        # Compile new centroids from reducer responses
        new_centroids = self.new_centroids  # Assuming all centroids have the same dimension
        counts = self.new_centroids_count

        for response in self.reducer_responses:
            print("here")
            print(response)
            for key, centroid_message in response.centroids.items():
                centroid_array = np.array(centroid_message.coordinates)  # Convert list to numpy array
                if centroid_array.shape[0] != new_centroids.shape[1]:
                    print(f"Shape mismatch detected: centroid key {key} has shape {centroid_array.shape}, expected {new_centroids.shape[1]}")
                    continue  # Skip this centroid to avoid crashing
                new_centroids[key] += centroid_array
                counts[key] += 1

        # Average the centroids
        for i in range(self.num_centroids):
            if counts[i] > 0:
                new_centroids[i] /= counts[i]
            
        print(f"Old centroids calculated: {self.centroids}")
        print(f"New centroids calculated: {new_centroids}")
        converge = await self.check_convergence(new_centroids)

        return converge

    async def check_convergence(self, new_centroids):
        # Calculate differences for each centroid individually
        differences = np.linalg.norm(self.centroids - new_centroids, axis=1)
        print(f"Individual differences: {differences}")
        # Check if all centroids have moved less than the threshold
        self.converged = np.all(differences < 0.001)
        if self.converged:
            print("Convergence reached for all centroids.")
        else:
            print("No convergence yet.")
        self.centroids=new_centroids
        return self.converged


    
    async def run(self):
        data, is_multiple = self.load_data()  # Adjusted to capture two return values
        if data is None:
            print("Failed to load data.")
            return

        if is_multiple:
            # If multiple files, data is already loaded as a single combined array
            self.initialize_centroids(data)
            data = self.file_paths
        else:
            # If a single file, data is loaded directly
            self.initialize_centroids(data)
        
        i = 0
        while not self.converged and i < self.max_iterations:
            print(f"Running iteration {i+1}")

            while True:
                # Check the health of mappers and reducers before dispatching tasks
                self.alive_mappers, self.alive_reducers = self.monitor_health()

                if len(self.alive_mappers.keys()) == 0:
                    print("No alive mappers available. Retrying...")
                    time.sleep(5)
                    continue

                if len(self.alive_reducers.keys()) == 0:
                    print("No alive reducers available. Retrying...")
                    time.sleep(5)
                    continue

                data_splits = self.split_data(data)

                success = await self.dispatch_and_process_tasks(data_splits)

                if not success:
                    print("One/Some mappers are off. Retrying...")
                    self.alive_mappers, self.alive_reducers = self.monitor_health()
                    time.sleep(5)
                    continue

                num_mappers, num_reducers = self.monitor_health()

                # Check if same number of reducers are alive
                if len(self.alive_mappers.keys()) != len(num_mappers.keys()):
                    self.alive_mappers, self.alive_reducers = self.monitor_health()
                    print("Number of mappers changed. Retrying...")
                    time.sleep(5)
                    continue

                # Check if same number of reducers are alive
                if len(self.alive_reducers.keys()) != len(num_reducers.keys()):
                    self.alive_mappers, self.alive_reducers = self.monitor_health()
                    print("Number of reducers changed. Retrying...")
                    time.sleep(5)
                    continue

                # If all is set, invoke reducers
                if len(self.alive_reducers.keys()) == len(num_reducers.keys()) and len(self.alive_mappers.keys()) == len(num_mappers.keys()):
                    print(f"Mappers done with tasks for iteration {i}, now moving on to reducers")
                    await self.invoke_reducers()
                    time.sleep(2)
                    self.new_centroids = np.zeros((self.num_centroids, len(self.centroids[0])))
                    self.new_centroids_count = np.zeros(self.num_centroids, dtype=int)
                    if (await self.handle_reducer_responses()):
                        break
                self.dump_centroids(self.converged)
                i += 1
                if i > self.max_iterations:
                    break
        if not self.converged:
            print(f"Reached max iterations, printing output in {self.outputfile}")
            with open(self.outputfile, 'w') as f:  # Open in append mode
                f.write(f"Centroids: {self.centroids}\n")
        else:
            print(f"Reached convergence, printing output in {self.outputfile}")
            self.dump_centroids(self.converged)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: master.py num_centroids max_iterations")
        sys.exit(1)

    num_centroids = int(sys.argv[1])
    max_iterations = int(sys.argv[2])

    master = Master(num_centroids, max_iterations)
    asyncio.run(master.run())