import sys
import numpy as np
import os
import grpc
import asyncio
import threading
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
        self.converged = False
        self.single_file_input = True
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
            return data
        elif len(files) > 1:
            print("multiple files input")
            self.single_file_input = False
            return files, False
        else:
            print("Input file/s missing")
            return None
    
    def split_data(self, data):
        active_mappers, _ = self.monitor_health()  # Get the currently available mappers
        num_active_mappers = len(active_mappers)

        if self.single_file_input:
            # Handle splitting for a single file input based on line indices
            num_data_points = len(data)
            if num_data_points < num_active_mappers:
                raise ValueError("Not enough data points relative to the number of available mappers")

            data_splits = []
            indices_per_mapper = num_data_points // num_active_mappers
            extra_data = num_data_points % num_active_mappers
            start_index = 0

            for mapper_id in active_mappers.keys():
                end_index = start_index + indices_per_mapper + (1 if extra_data > 0 else 0)
                if extra_data > 0:
                    extra_data -= 1
                data_splits.append((mapper_id, start_index, end_index))
                start_index = end_index
        else:
            # Handle multiple files by distributing files among the available mappers
            if len(data) < num_active_mappers:
                raise ValueError("Not enough files relative to the number of available mappers")
            data_splits = []
            files_per_mapper = len(data) // num_active_mappers
            extra_files = len(data) % num_active_mappers
            data_index = 0

            for mapper_id in active_mappers.keys():
                files_for_this_mapper = data[data_index:data_index + files_per_mapper + (1 if extra_files > 0 else 0)]
                data_splits.append((mapper_id, files_for_this_mapper))
                data_index += len(files_for_this_mapper)
                if extra_files > 0:
                    extra_files -= 1

        return data_splits
    
    async def dispatch_tasks(self, data_splits):
        tasks = []
        for split_info in data_splits:
            mapper_id = split_info[0]
            if self.single_file_input:
                _, start_index, end_index = split_info
                task = mapreduce_pb2.MapperTask(
                    centroids=np.array(self.centroids).flatten().tolist(),
                    input_data_paths=None,
                    single_input_file=self.single_file_path,
                    start_index=start_index,
                    end_index=end_index,
                    task_id=str(mapper_id),
                    num_partitions=len(self.alive_reducers.keys())
                )
            else:
                _, files = split_info
                task = mapreduce_pb2.MapperTask(
                    centroids=np.array(self.centroids).flatten().tolist(),
                    input_data_paths=files,
                    single_input_file=None,
                    start_index=-1,
                    end_index=-1,
                    task_id=str(mapper_id),
                    num_partitions=len(self.alive_reducers.keys())
                )
            tasks.append(self.process_task(task, mapper_id))
        
        results = await asyncio.gather(*tasks)
        if all(result.success for result in results):  # Check if all tasks were successful
            print("All tasks completed successfully.")
            return True
        else:
            print("Some tasks failed. Retrying...")
            await asyncio.sleep(5)  # Wait before retrying
            return False
    
    async def process_task(self, task, mapper_id):
        address = self.mappers[mapper_id]
        async with grpc.aio.insecure_channel(address) as channel:
            stub = mapreduce_pb2_grpc.MapperServiceStub(channel)
            try:
                response = await stub.ProcessInput(task)
                print(f"Task sent to mapper {mapper_id}. Response: {response.message}")
            except grpc.aio.AioRpcError as e:
                print(f"Failed to send task to mapper {mapper_id}: {str(e)}")
                # Optionally, handle reassignment here

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

    def create_grpc_stub(self, address):
        channel = grpc.insecure_channel(address)
        return mapreduce_pb2_grpc.MapperServiceStub(channel)
    
    async def invoke_reducers(self):
        self.reducer_responses = []
        keys_per_reducer = defaultdict(list)
        centroid_indices = list(range(self.num_centroids))

        for i, key in enumerate(centroid_indices):
            reducer_id = list(self.alive_reducers.keys())[i % len(self.alive_reducers.keys())]
            keys_per_reducer[reducer_id].append(key)

        tasks = []
        for reducer_id, address in self.alive_reducers.items():
            task = self.process_reducer_task(
                list(self.alive_mappers.keys()),
                keys_per_reducer[reducer_id],
                reducer_id,
                address
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

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
                self.reducer_responses.append(response)
                print(f"Task sent to reducer {reducer_id}. Response: {response.message}")
            except grpc.aio.AioRpcError as e:
                print(f"Failed to send task to reducer {reducer_id}: {str(e)}")
    
    def handle_reducer_responses(self):
        # Compile new centroids from reducer responses
        new_centroids = np.zeros((self.num_centroids, len(self.centroids[0])))  # Assuming all centroids have the same dimension
        counts = np.zeros(self.num_centroids)

        for response in self.reducer_responses:
            for key, centroid in response.centroids.items():
                new_centroids[key] += centroid
                counts[key] += 1

        # Average the centroids
        for i in range(self.num_centroids):
            if counts[i] > 0:
                new_centroids[i] /= counts[i]

        print(f"New centroids calculated: {new_centroids}")
        self.check_convergence(new_centroids)

        return self.check_convergence()

    def check_convergence(self, new_centroids):
        # Placeholder for actual convergence checking logic
        # This could involve checking if the centroids have moved less than a certain threshold
        difference = np.linalg.norm(self.centroids - new_centroids)
        self.converged = difference < 0.001  # Example threshold
        if self.converged:
            print("Convergence reached.")
        else:
            print("No convergence yet.")
        return self.converged

    
    async def run(self):
        data = self.load_data()
        if data is None:
            return

        if self.single_file_input:
            self.initialize_centroids(data)
        else:
            self.initialize_centroids(np.loadtxt(data[0], delimiter=','))  # Assuming data loading for centroids from the first file if multiple
        
        i = 0
        while not self.converged and i < self.max_iterations:
            print(f"Running iteration {i+1}")

            while True:
                # Check the health of mappers and reducers before dispatching tasks
                self.alive_mappers, self.alive_reducers = self.monitor_health()

                if len(self.alive_mappers.keys()) == 0:
                    print("No alive mappers available. Retrying...")
                    await asyncio.sleep(5)  # Wait before retrying
                    continue

                if len(self.alive_reducers.keys()) == 0:
                    print("No alive reducers available. Retrying...")
                    await asyncio.sleep(5)  # Wait before retrying
                    continue

                data_splits = self.split_data(data)

                success = await self.dispatch_tasks(data_splits)

                if not success:
                    print("One/Some mappers are off. Retrying...")
                    self.alive_mappers, self.alive_reducers = self.monitor_health()
                    await asyncio.sleep(5)  # Wait before retrying
                    continue

                num_mappers, num_reducers = self.monitor_health()

                # Check if same number of reducers are alive
                if len(self.alive_mappers.keys()) != num_mappers:
                    self.alive_mappers, self.alive_reducers = self.monitor_health()
                    print("Number of mappers changed. Retrying...")
                    await asyncio.sleep(5)  # Wait before retrying
                    continue

                # Check if same number of reducers are alive
                if len(self.alive_reducers.keys()) != num_reducers:
                    self.alive_mappers, self.alive_reducers = self.monitor_health()
                    print("Number of reducers changed. Retrying...")
                    await asyncio.sleep(5)  # Wait before retrying
                    continue

                # If all is set, invoke reducers
                if len(self.alive_reducers.keys()) == num_reducers and len(self.alive_mappers.keys()) == num_mappers:
                    await self.invoke_reducers()
                    if (self.handle_reducer_responses()):
                        break
            self.dump_centroids()
            i += 1
        if not self.converged:
            print(f"Reached max iterations, printing output in {self.outputfile}")
            with open(self.outputfile, 'w') as f:  # Open in append mode
                f.write(f"Centroids: {self.centroids}\n")
        else:
            print(f"Reached convergence, printing output in {self.outputfile}")
            self.dump_centroids()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: master.py num_centroids max_iterations")
        sys.exit(1)

    num_centroids = int(sys.argv[1])
    max_iterations = int(sys.argv[2])

    master = Master(num_centroids, max_iterations)
    asyncio.run(master.run())