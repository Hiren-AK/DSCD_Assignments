import pika
import sys
import json
import ssl

class Youtuber:
    def __init__(self, youtuber_name):
        self.youtuber_name = youtuber_name
        # Set up the SSL context to connect securely
        context = ssl.create_default_context(cafile="/path/to/ca_certificate.pem")
        context.load_cert_chain("/path/to/cert.pem", "/path/to/key.pem")
        # Establish a connection to the remote RabbitMQ server on Google Cloud
        credentials = pika.PlainCredentials('youtuber', 'youtuber23')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='34.68.235.110',  # Replace with your Google Cloud VM's public IP address
                credentials=credentials,
                ssl_options=pika.SSLOptions(context),
                port=5671  # Default SSL port for RabbitMQ
            )
        )
        self.channel = self.connection.channel()

        # Declare the queue for YouTuber requests if it doesn't already exist
        self.channel.queue_declare(queue='youtuber_requests')

    def publish_video(self, video_name):
        # Construct the message with the YouTuber's name and video name
        message = json.dumps({
            'youtuber': self.youtuber_name,
            'videoName': video_name
        })
        # Publish the message to the 'youtuber_requests' queue
        self.channel.basic_publish(
            exchange='',
            routing_key='youtuber_requests',
            body=message
        )
        print("SUCCESS: Video published to the server.")

    def close_connection(self):
        # Close the connection to the RabbitMQ server
        self.connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: Youtuber.py [YoutuberName] [VideoName]")
        sys.exit(1)

    youtuber_name = sys.argv[1]
    video_name = ' '.join(sys.argv[2:])

    youtuber = Youtuber(youtuber_name)
    youtuber.publish_video(video_name)
    youtuber.close_connection()
