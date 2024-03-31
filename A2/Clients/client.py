import grpc
import raftkv_pb2
import raftkv_pb2_grpc

# Configuration: IP addresses and ports of all the nodes in the Raft cluster
node_addresses = {
    '01': 'localhost:50051',
    '02': 'localhost:50052',
    '03': 'localhost:50053',
}

current_leader_id = None  # Initially, the leader ID is unknown

def get_channel(node_id):
    """Returns a gRPC channel for the given node ID."""
    return grpc.insecure_channel(node_addresses[node_id])

def send_request(request):
    """Sends a request to the Raft cluster and handles leader redirection."""
    global current_leader_id

    # If we don't know who the leader is, try all nodes until one succeeds or redirects us correctly
    if not current_leader_id:
        for node_id in node_addresses:
            response, success = attempt_request_on_node(node_id, request)
            if success:
                return response
            else:
                try:
                    temp_lead = response.leaderID
                    temp_addr = response.data
                    if (temp_lead in node_addresses and node_addresses[temp_lead] == temp_addr):
                        response, success = attempt_request_on_node(temp_lead, request)
                        if success:
                            current_leader_id = temp_lead
                            return response
                except:
                    print("Error while sending/routing send request")
                
    else:
        # Try the current leader first
        response, success = attempt_request_on_node(current_leader_id, request)
        if success:
            return response
        else:
            current_leader_id = None

    print("Failed to get a successful response from the cluster.")
    return None

def attempt_request_on_node(node_id, request):
    """Attempts to send the request to a specified node and handles the response."""
    global current_leader_id
    try:
        with get_channel(node_id) as channel:
            stub = raftkv_pb2_grpc.KeyValueStoreStub(channel)
            response = stub.ServeClient(raftkv_pb2.ServeClientArgs(request=request))
            if response and response.success:
                print(f"Request successful: {response.data}")
                current_leader_id = response.leaderID  # Update current leader ID based on the response
                return response, True
            else:
                print(f"Node {node_id} is not the leader, redirecting to {response.leaderID}.")
                current_leader_id = response.leaderID  # Update current leader ID for redirection
    except grpc.RpcError as e:
        print(f"RPCError on node {node_id}")
    return None, False

def main():
    print("Raft Cluster Client")
    print("Available commands:")
    print("  SET <key> <value> - Sets the value for a key")
    print("  GET <key> - Retrieves the value for a key")
    print("  EXIT - Exits the application")

    while True:
        try:
            command = input("Enter command: ").strip()
            if command.upper() == "EXIT":
                print("Exiting the client.")
                break
            elif command.startswith("SET"):
                _, key, value = command.split(maxsplit=2)
                set_request = f"SET {key} {value}"
                print(f"Sending SET request for {key}...")
                response = send_request(set_request)
                if (response.success and response.data == set_request):
                    print("success: ", set_request)
            elif command.startswith("GET"):
                _, key = command.split(maxsplit=1)
                get_request = f"GET {key}"
                print(f"Sending GET request for {key}...")
                response = send_request(get_request)
                if response:
                    print(f"Value for {key}: {response.data}")
                else:
                    print("Failed to retrieve the value.")
            else:
                print("Unknown command. Please try again.")
        except ValueError as e:
            print(f"Error processing command: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()