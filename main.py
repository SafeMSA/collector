import pika
import socket
import time
import json
import os
from datetime import datetime
from collections import defaultdict

# Constants
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 9093
SUBSCRIBER_QUEUE = 'response_queue'  # Name of the queue to send the response to
USER = 'myuser'
PASSWORD = 'mypassword'

data = defaultdict(list)

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
    global data
    data[message.get("tag")].append((message.get("id"),message.get("time_diff")))

    if (int(message.get("id")) == 10):
        for k,v in data.items:
            # Open a text file in write mode
            with open(f'{k}.txt', 'w') as file:
                # Write headers
                file.write("ID,Time Difference\n")
                
                # Iterate through both lists and write each pair to the file
                for (id,td) in v:
                    file.write(f"{id},{td}\n")

        print("Data saved to files")
    
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