import pika
import sys
import json

class User:
    def __init__(self, username):
        self.username = username
        # Set up the SSL context to connect securely
        context = ssl.create_default_context(cafile="/path/to/ca_certificate.pem")
        context.load_cert_chain("/path/to/cert.pem", "/path/to/key.pem")
        # Establish a connection to the remote RabbitMQ server on Google Cloud
        credentials = pika.PlainCredentials('user1', 'user1')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='34.68.235.110',  # Replace with your Google Cloud VM's public IP address
                credentials=credentials,
                ssl_options=pika.SSLOptions(context),
                port=5671  # Default SSL port for RabbitMQ
            )
        )
        self.channel = self.connection.channel()

        # Declare a queue for this user to receive notifications
        self.channel.queue_declare(queue=self.username)

    def update_subscription(self, action, youtuber):
        # Send a subscription or unsubscription message to the server
        subscribe_status = True if action == 'subscribe' else False
        message = json.dumps({
            'user': self.username,
            'youtuber': youtuber,
            'subscribe': subscribe_status  # The server expects 'subscribe' key
        })
        self.channel.basic_publish(
            exchange='',
            routing_key='user_requests',  # Make sure this matches the server queue name for user requests
            body=message
        )
        action_text = 'subscribed to' if action == 'subscribe' else 'unsubscribed from'
        print(f"SUCCESS: {self.username} has {action_text} {youtuber}.")



    def receive_notifications(self):
        # Define the callback for processing notifications
        def callback(ch, method, properties, body):
            notification = json.loads(body)
            print(f"New Notification: {notification['youtuber']} uploaded {notification['videoName']}")

        # Start consuming notifications from this user's queue
        self.channel.basic_consume(
            queue=self.username,
            on_message_callback=callback,
            auto_ack=True
        )
        print(f"Listening for notifications for {self.username}...")
        self.channel.start_consuming()

    def close_connection(self):
        # Close the connection to the RabbitMQ server
        self.connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: User.py [Username] [Action: s/u] [YoutuberName]")
        sys.exit(1)

    username = sys.argv[1]
    user = User(username)

    # Check for action and execute accordingly
    if len(sys.argv) == 4:
        action = 'subscribe' if sys.argv[2] == 's' else 'unsubscribe'
        youtuber = sys.argv[3]
        user.update_subscription(action, youtuber)

    # Always listen for notifications
    try:
        user.receive_notifications()
    except KeyboardInterrupt:
        user.close_connection()
