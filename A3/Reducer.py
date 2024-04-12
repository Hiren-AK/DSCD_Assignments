import grpc
from concurrent import futures
import time

import mapreduce_pb2
import mapreduce_pb2_grpc

class Reducer(mapreduce_pb2_grpc.ReducerServiceServicer):

    def ProcessIntermediateData(self, request, context):
        print("Processing data for task ID:", request.task_id)
        # Dummy implementation: assume reducer does its job
        success = True
        message = "Reduction completed successfully"
        output_data_path = "/path/to/final/output"
        return mapreduce_pb2.ReducerResponse(
            success=success,
            message=message,
            output_data_path=output_data_path,
            task_id=request.task_id
        )

    def FetchIntermediateData(self, request, context):
        print("Fetching data from paths:", request.data_paths)
        # Dummy response with fake data
        data = ["data1", "data2", "data3"]
        return mapreduce_pb2.FetchResponse(
            data=data
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_ReducerServiceServicer_to_server(Reducer(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
