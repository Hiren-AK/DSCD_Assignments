# gRPC 

## Market System gRPC Services
This README provides instructions on compiling the provided gRPC services using the included Makefile and an overview of the code structure of a market system that includes functionalities for sellers, buyers, notifications, and search.

### Compiling with Makefile
Update Makefile: Open the Makefile included in the project directory.
Modify Makefile: Update the PROTO_DIR variable to point to the directory where your .proto files (market_service.proto and notify_service.proto) are located.
Compile: Run make all in your terminal. This command will compile the .proto files into Python code using the gRPC tools.
Generated Files: The compiled Python files will be generated in the same directory as your .proto files.

#### Prerequisites
- Ensure Python and gRPC tools are installed: You must have Python installed on your system along with the necessary gRPC tools.
- Install gRPC for Python: If gRPC for Python is not already installed, you can install it using the command:
```bash
pip install grpcio grpcio-tools
```

##### Code Overview
The provided code consists of gRPC services for a market system, detailed as follows:

market_service.proto: Defines the gRPC service and message types for market-related functionalities like registering sellers and buyers, selling items, updating items, etc.
notify_service.proto: Defines the gRPC service and message types for sending notifications to sellers and buyers.
buyer.py: Implements a client for the market system, allowing buyers to register, login, search for items, buy items, rate items, and manage notifications as a gRPC server for receiving notifications.
market.py: Implements the gRPC server for market-related functionalities.
seller.py: Implements another type of client for the market system, allowing sellers to register, login, sell items, update items, delete items, and manage notifications as a gRPC server for receiving notifications.
Makefile: A Makefile is provided to compile the .proto files into Python code using the gRPC tools.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# ZeroMQ


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# RabbitMQ


YouTube Notification System with RabbitMQ

Clone the Repository:
git clone <repository_url>

Install Dependencies:
pip install -r requirements.txt

RabbitMQ Installation:

Install RabbitMQ on your system or use a cloud-based RabbitMQ service.
Ensure RabbitMQ is running and accessible.

The system consists of the following components:

1. User Application (User.py): Allows users to subscribe, unsubscribe, and receive notifications for videos uploaded by subscribed YouTubers.
2. YouTuber Application (YouTuber.py): Enables YouTubers to publish videos, which are processed by the server, and notifications are sent to subscribed users.
3. YouTube Server (YouTubeServer.py): Central server responsible for handling user requests, YouTuber uploads, and notification delivery.


Run the YouTube Server:
python YouTubeServer.py

Users can subscribe or unsubscribe to YouTubers and receive notifications:

python User.py <username> <action> <youtuber_name>

YouTubers can publish videos:

python YouTuber.py <youtuber_name> <video_name>

Prerequisites
Ensure Python and RabbitMQ are installed on your system.
Make sure RabbitMQ is running and accessible.