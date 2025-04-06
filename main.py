import pika
import socket
import time
import json
import os
from datetime import datetime

# Constants
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 9093
SUBSCRIBER_QUEUE = 'response_queue'  # Name of the queue to send the response to
USER = 'myuser'
PASSWORD = 'mypassword'

ids = []
time_diffs = []

# Connect to RabbitMQ and declare the necessary queues
def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(USER, PASSWORD)
    
    while True:
        try:
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                port=RABBITMQ_PORT,
                credentials=credentials, 
                blocked_connection_timeout=1
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declare the subscriber queue
            channel.queue_declare(queue=SUBSCRIBER_QUEUE, durable=True)

            return connection, channel
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as error:
            print(f"Error connecting to RabbitMQ: {error}. Retrying in 5 seconds...")
            time.sleep(5)

# Callback to handle incoming messages
def callback(ch, method, properties, body):
    message = json.loads(body.decode())  # Parse incoming JSON message
    print(f"Received: {message}")
    
    # Extract id and timestamp from the received message
    global ids
    global time_diffs

    ids.append(message.get("id"))
    time_diffs.append(message.get("time_diff"))

    if (int(message.get("id")) == 10):
        # Open a text file in write mode
        with open('output.txt', 'w') as file:
            # Write headers
            file.write("ID,Time Difference\n")
            
            # Iterate through both lists and write each pair to the file
            for i in range(len(ids)):
                file.write(f"{ids[i]},{time_diffs[i]}\n")

        print("Data saved to output.txt")
    
    # Acknowledge the received message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    while True:
        try:
            # Connect to RabbitMQ
            connection, channel = connect_to_rabbitmq()

            # Start consuming messages from the subscriber queue
            channel.basic_consume(queue=SUBSCRIBER_QUEUE, on_message_callback=callback)

            print("Waiting for messages. To exit press CTRL+C.")
            channel.start_consuming()

        except Exception as e:
            print(f"Error: {e}. Reconnecting...")
            time.sleep(5)

if __name__ == '__main__':
    main()